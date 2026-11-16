"""跨市场设备服务质量分析：publication模块。"""
import csv
import io
import json
import uuid
from .analytics import Analytics, days_between
from .errors import require
from .store import Store, now


class Publication:
    def __init__(self, store: Store):
        self.store = store
        self.analytics = Analytics(store)

    @staticmethod
    def head(db, market: str) -> int:
        row = db.execute("SELECT version FROM publication_heads WHERE market=?", (market,)).fetchone()
        return row[0] if row else 0

    def prepare(self, market: str, from_day: str, to_day: str, expected_head: int) -> dict:
        require(type(expected_head) is int and expected_head >= 0, "invalid_head", "expected head required")
        list(days_between(from_day, to_day))
        with self.store.transaction() as db:
            require(db.execute("SELECT 1 FROM markets WHERE code=?", (market,)).fetchone(), "unknown_market", "market unknown", 404)
            head = self.head(db, market)
            require(head == expected_head, "stale_head", "published head changed", 409)
            revision = self.store.revision(db)
            candidate_id = uuid.uuid4().hex
            rows = self.analytics.calculate(db, market, from_day, to_day)
            db.execute("INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?)", (candidate_id, market, from_day, to_day, revision, head, "prepared", now()))
            for row in rows:
                db.execute("INSERT INTO candidate_rows VALUES(?,?,?,?)", (candidate_id, row["day"], row["product"], json.dumps(row, sort_keys=True)))
            self.store.audit(db, "candidate_prepared", candidate_id, {"source_revision": revision, "expected_head": head, "rows": len(rows)})
            return {"id": candidate_id, "market": market, "source_revision": revision, "expected_head": head, "rows": len(rows), "status": "prepared"}

    def candidate(self, candidate_id: str) -> dict:
        with self.store.read() as db:
            row = db.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            require(row is not None, "unknown_candidate", "candidate unknown", 404)
            output = dict(row)
            output["rows"] = [json.loads(r[0]) for r in db.execute("SELECT payload FROM candidate_rows WHERE candidate_id=? ORDER BY day,product", (candidate_id,))]
            output["current_source_revision"] = self.store.revision(db)
            output["current_head"] = self.head(db, row["market"])
            output["ready"] = (output["status"] == "prepared" and output["source_revision"] == output["current_source_revision"] and output["expected_head"] == output["current_head"])
            # 显式标注失效原因：撤销/重放会推进源修订，使未发布候选识别为源修订失效。
            output["invalid_reasons"] = []
            if output["status"] != "prepared":
                output["invalid_reasons"].append("candidate_" + output["status"])
            if output["source_revision"] != output["current_source_revision"]:
                output["invalid_reasons"].append("source_revision_superseded")
            if output["expected_head"] != output["current_head"]:
                output["invalid_reasons"].append("head_superseded")
            return output

    def discard(self, candidate_id: str) -> dict:
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            require(row is not None, "unknown_candidate", "candidate unknown", 404)
            require(row["status"] == "prepared", "candidate_not_prepared", "candidate unavailable", 409)
            db.execute("UPDATE candidates SET status='discarded' WHERE id=?", (candidate_id,))
            self.store.audit(db, "candidate_discarded", candidate_id, {})
            return {"id": candidate_id, "status": "discarded"}

    def publish(self, candidate_id: str, expected_head: int, expected_source_revision: int) -> dict:
        require(type(expected_head) is int and type(expected_source_revision) is int, "invalid_revision", "integer revisions required")
        with self.store.transaction() as db:
            candidate = db.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            require(candidate is not None, "unknown_candidate", "candidate unknown", 404)
            require(candidate["status"] == "prepared", "candidate_not_prepared", "candidate unavailable", 409)
            require(candidate["expected_head"] == expected_head == self.head(db, candidate["market"]), "stale_head", "published head changed", 409)
            require(candidate["source_revision"] == expected_source_revision == self.store.revision(db), "stale_source", "source revision changed", 409)
            version = expected_head + 1
            market = candidate["market"]
            db.execute("INSERT INTO publications VALUES(?,?,?,?,?)", (market, version, candidate_id, expected_source_revision, now()))
            db.execute("INSERT INTO publication_rows(market,version,day,product,payload) SELECT ?,?,day,product,payload FROM candidate_rows WHERE candidate_id=?", (market, version, candidate_id))
            db.execute("INSERT INTO publication_heads(market,version) VALUES(?,?) ON CONFLICT(market) DO UPDATE SET version=excluded.version", (market, version))
            db.execute("UPDATE candidates SET status='published' WHERE id=?", (candidate_id,))
            self.store.audit(db, "published", market, {"version": version, "candidate": candidate_id, "source_revision": expected_source_revision})
            return {"market": market, "version": version, "candidate_id": candidate_id, "source_revision": expected_source_revision}

    def read(self, market: str, version: int) -> dict:
        with self.store.read() as db:
            meta = db.execute("SELECT * FROM publications WHERE market=? AND version=?", (market, version)).fetchone()
            require(meta is not None, "unknown_publication", "publication unknown", 404)
            rows = db.execute("SELECT payload FROM publication_rows WHERE market=? AND version=? ORDER BY day,product", (market, version)).fetchall()
            return {**dict(meta), "rows": [json.loads(r[0]) for r in rows]}

    def versions(self, market: str) -> list[dict]:
        with self.store.read() as db:
            require(db.execute("SELECT 1 FROM markets WHERE code=?", (market,)).fetchone(), "unknown_market", "market unknown", 404)
            return [dict(row) for row in db.execute("SELECT * FROM publications WHERE market=? ORDER BY version", (market,))]

    def diff(self, market: str, old: int, new: int) -> dict:
        previous = self.read(market, old)
        current = self.read(market, new)
        left = {(r["day"], r["product"]): r for r in previous["rows"]}
        right = {(r["day"], r["product"]): r for r in current["rows"]}
        changes = []
        for key in sorted(left.keys() | right.keys()):
            if left.get(key) != right.get(key):
                changes.append({"day": key[0], "product": key[1], "before": left.get(key), "after": right.get(key)})
        return {"market": market, "old": old, "new": new, "changes": changes}

    def export_csv(self, market: str, version: int) -> str:
        release = self.read(market, version)
        buffer = io.StringIO()
        fields = ["day", "market", "product", "installed_devices", "active_devices", "exposure_hours", "ticket_opened", "ticket_closed", "ticket_open_at_end", "critical_opened", "acknowledged", "response_hours_total", "telemetry_events", "devices_reporting", "run_hours", "downtime_hours", "telemetry_coverage", "tickets_per_100_devices", "downtime_share"]
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(release["rows"])
        return buffer.getvalue()
