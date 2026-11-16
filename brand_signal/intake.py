"""跨市场设备服务质量分析：intake模块。"""
import hashlib
import json
import uuid
from .errors import DomainError, require
from .service import Service
from .store import Store, now
from .telemetry import Telemetry
from .contracts import Contracts
from .mappings import Mappings
from .revocations import Revocations, REPLAY_NOT_CORRECTED


SOURCES = {"tickets", "telemetry"}


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Intake:
    def __init__(self, store: Store):
        self.store = store
        self.service = Service(store)
        self.telemetry = Telemetry(store)
        self.contracts = Contracts(store)
        self.mappings = Mappings(store)
        self.revocations = Revocations(store)

    def _mapped(self, db, source: str, row: dict) -> dict:
        if "external_device_id" not in row:
            return row
        require("device_id" not in row, "ambiguous_device", "external and canonical device id both supplied")
        timestamp = row.get("opened_at") if source == "tickets" else row.get("observed_at")
        resolved = self.mappings.resolve(db, source, row["external_device_id"], timestamp)
        return {**row, "device_id": resolved}

    def preview(self, source: str, rows: list[dict]) -> dict:
        require(source in SOURCES, "invalid_source", "source unknown")
        require(isinstance(rows, list) and 1 <= len(rows) <= 500, "invalid_batch", "batch needs 1..500 rows")
        outcomes = []
        seen = set()
        with self.store.read() as db:
            for index, row in enumerate(rows):
                try:
                    require(isinstance(row, dict), "invalid_row", "row object required")
                    key = row.get("event_key")
                    require(isinstance(key, str) and 1 <= len(key) <= 100, "invalid_event_key", "event key required")
                    require(key not in seen, "duplicate_in_batch", "event key repeated in batch")
                    seen.add(key)
                    existing = db.execute("SELECT digest FROM source_events WHERE source=? AND event_key=?", (source, key)).fetchone()
                    require(existing is None, "event_conflict", "event key already accepted", 409)
                    normalized, _ = self.contracts.normalize(db, source, row)
                    normalized = self._mapped(db, source, normalized)
                    (self.service.validate_open if source == "tickets" else self.telemetry.validate)(db, normalized)
                    outcomes.append({"row": index, "status": "ready"})
                except DomainError as exc:
                    outcomes.append({"row": index, "status": "rejected", "reason": exc.code})
        return {"source": source, "rows": outcomes, "ready": sum(r["status"] == "ready" for r in outcomes)}

    def submit(self, source: str, request_key: str, rows: list[dict], corrects: str | None = None) -> dict:
        require(source in SOURCES, "invalid_source", "source unknown")
        require(isinstance(request_key, str) and 1 <= len(request_key) <= 100, "invalid_request_key", "request key required")
        require(isinstance(rows, list) and 1 <= len(rows) <= 500, "invalid_batch", "batch needs 1..500 rows")
        require(corrects is None or isinstance(corrects, str), "invalid_corrects", "correction target must be a batch id")
        try:
            body_digest = digest(rows)
        except (TypeError, ValueError, OverflowError) as exc:
            raise DomainError("invalid_batch", "batch must contain JSON values") from exc
        with self.store.transaction() as db:
            if corrects is not None:
                target = db.execute("SELECT * FROM batches WHERE id=?", (corrects,)).fetchone()
                require(target is not None, "unknown_batch", "correction target batch unknown", 404)
                require(target["source"] == source, "invalid_corrects", "correction target is for another source", 409)
                revoked = db.execute("SELECT 1 FROM revocations WHERE batch_id=?", (corrects,)).fetchone()
                require(revoked is not None, "batch_not_revoked", "correction target must first be revoked", 409)
            prior = db.execute("SELECT * FROM batches WHERE source=? AND request_key=?", (source, request_key)).fetchone()
            if prior:
                require(prior["digest"] == body_digest, "request_conflict", "request key used with different payload", 409)
                result = self._result(db, prior["id"])
                result["replay"] = True
                return result
            batch_id = uuid.uuid4().hex
            db.execute("INSERT INTO batches(id,source,request_key,digest,status,created_at,corrects) VALUES(?,?,?,?,?,?,?)", (batch_id, source, request_key, body_digest, "processing", now(), corrects))
            allowed_keys = self.revocations.corrected_keys(db, corrects) if corrects else None
            seen = set()
            for index, row in enumerate(rows):
                key = row.get("event_key") if isinstance(row, dict) else None
                status, reason = "accepted", None
                db.execute("SAVEPOINT row_input")
                try:
                    require(isinstance(row, dict), "invalid_row", "row object required")
                    require(isinstance(key, str) and 1 <= len(key) <= 100, "invalid_event_key", "event key required")
                    require(key not in seen, "duplicate_in_batch", "event key repeated in batch")
                    seen.add(key)
                    if allowed_keys is not None:
                        require(key in allowed_keys, REPLAY_NOT_CORRECTED, "event key was not part of the revoked batch")
                    row_digest = digest(row)
                    prior_event = db.execute("SELECT * FROM source_events WHERE source=? AND event_key=?", (source, key)).fetchone()
                    require(prior_event is None, "event_conflict", "event key already accepted", 409)
                    normalized, contract_version = self.contracts.normalize(db, source, row)
                    normalized = self._mapped(db, source, normalized)
                    supersede = corrects is not None
                    accepted = (self.service.accept_open if source == "tickets" else self.telemetry.accept)(db, normalized, batch_id, supersede)
                    db.execute("INSERT INTO source_events VALUES(?,?,?,?)", (source, key, row_digest, accepted["id"]))
                    self.store.audit(db, "row_contract", accepted["id"], {"source": source, "contract_version": contract_version, "batch": batch_id, "corrects": corrects})
                    db.execute("RELEASE row_input")
                except DomainError as exc:
                    db.execute("ROLLBACK TO row_input")
                    db.execute("RELEASE row_input")
                    status, reason = "rejected", exc.code
                db.execute("INSERT INTO batch_rows VALUES(?,?,?,?,?,?)", (batch_id, index, key, status, reason, canonical(row)))
            accepted = db.execute("SELECT count(*) FROM batch_rows WHERE batch_id=? AND status='accepted'", (batch_id,)).fetchone()[0]
            rejected = len(rows) - accepted
            db.execute("UPDATE batches SET status='completed',accepted=?,rejected=? WHERE id=?", (accepted, rejected, batch_id))
            self.store.audit(db, "batch_completed", batch_id, {"source": source, "accepted": accepted, "rejected": rejected, "corrects": corrects})
            result = self._result(db, batch_id)
            result["replay"] = False
            result["corrects"] = corrects
            return result

    @staticmethod
    def _result(db, batch_id: str) -> dict:
        batch = dict(db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())
        batch["rows"] = [dict(row) for row in db.execute("SELECT row_no,event_key,status,reason FROM batch_rows WHERE batch_id=? ORDER BY row_no", (batch_id,))]
        revocation = db.execute("SELECT id,reason_code,status FROM revocations WHERE batch_id=?", (batch_id,)).fetchone()
        batch["revocation"] = dict(revocation) if revocation else None
        return batch

    def batch(self, batch_id: str) -> dict:
        with self.store.read() as db:
            require(db.execute("SELECT 1 FROM batches WHERE id=?", (batch_id,)).fetchone(), "unknown_batch", "batch unknown", 404)
            return self._result(db, batch_id)
