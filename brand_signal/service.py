"""跨市场设备服务质量分析：service模块。"""
from .catalog import Catalog, iso, instant
from .errors import require
from .store import Store


SEVERITIES = {"low", "normal", "high", "critical"}
TRANSITIONS = {
    "open": {"acknowledge": "acknowledged", "cancel": "cancelled"},
    "acknowledged": {"resolve": "resolved", "reopen": "open"},
    "resolved": {"close": "closed", "reopen": "open"},
    "closed": {"reopen": "open"},
    "cancelled": {},
}


class Service:
    def __init__(self, store: Store):
        self.store = store

    def validate_open(self, db, event: dict, supersede_revoked: bool = False) -> dict:
        require(isinstance(event, dict), "invalid_ticket", "ticket object required")
        ticket_id = event.get("id")
        device_id = event.get("device_id")
        require(isinstance(ticket_id, str) and 1 <= len(ticket_id) <= 100, "invalid_ticket_id", "ticket id required")
        require(isinstance(device_id, str), "invalid_device", "device id required")
        opened = iso(event.get("opened_at"))
        Catalog.device(db, device_id, opened)
        severity = event.get("severity", "normal")
        require(severity in SEVERITIES, "invalid_severity", "severity unknown")
        existing = db.execute("SELECT state FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        if existing is not None:
            require(existing["state"] == "revoked" and supersede_revoked, "ticket_exists", "ticket id exists", 409)
        return {"id": ticket_id, "device_id": device_id, "opened_at": opened, "severity": severity}

    def accept_open(self, db, event: dict, batch_id: str, supersede_revoked: bool = False) -> dict:
        data = self.validate_open(db, event, supersede_revoked)
        existing = db.execute("SELECT * FROM tickets WHERE id=?", (data["id"],)).fetchone()
        if existing is not None:
            # 修正重放：仅允许取代同一业务键下已撤销的记录，活跃记录仍按冲突拒收。
            require(existing["state"] == "revoked" and supersede_revoked, "ticket_exists", "ticket id exists", 409)
            next_revision = existing["revision"] + 1
            db.execute(
                "UPDATE tickets SET device_id=?,opened_at=?,severity=?,state='open',closed_at=NULL,revision=?,origin_batch=? WHERE id=?",
                (data["device_id"], data["opened_at"], data["severity"], next_revision, batch_id, data["id"]),
            )
            db.execute(
                "INSERT INTO ticket_history VALUES(?,?,?,?,?,?)",
                (data["id"], next_revision, data["opened_at"], "reopen", "open", "correction_replay"),
            )
            db.execute("UPDATE ticket_revocations SET status='superseded' WHERE ticket_id=?", (data["id"],))
            revision = self.store.bump(db)
            self.store.audit(db, "ticket_replayed", data["id"], {"device": data["device_id"], "batch": batch_id, "source_revision": revision})
            return {**data, "state": "open", "revision": next_revision}
        db.execute("INSERT INTO tickets(id,device_id,opened_at,severity,state,origin_batch) VALUES(?,?,?,?,?,?)", (data["id"], data["device_id"], data["opened_at"], data["severity"], "open", batch_id))
        db.execute("INSERT INTO ticket_history VALUES(?,?,?,?,?,?)", (data["id"], 1, data["opened_at"], "open", "open", ""))
        revision = self.store.bump(db)
        self.store.audit(db, "ticket_opened", data["id"], {"device": data["device_id"], "source_revision": revision})
        return {**data, "state": "open", "revision": 1}

    def transition(self, ticket_id: str, action: str, at: str, expected_revision: int, note: str = "") -> dict:
        moment = iso(at)
        require(isinstance(note, str) and len(note) <= 1000, "invalid_note", "note too long")
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            require(row is not None, "unknown_ticket", "ticket unknown", 404)
            require(row["state"] != "revoked", "ticket_revoked", "ticket was revoked as an invalid source", 409)
            require(row["revision"] == expected_revision, "stale_ticket", "ticket revision changed", 409)
            target = TRANSITIONS[row["state"]].get(action)
            require(target is not None, "invalid_transition", "action unavailable in current state", 409)
            last = db.execute("SELECT event_at FROM ticket_history WHERE ticket_id=? ORDER BY revision DESC LIMIT 1", (ticket_id,)).fetchone()
            require(instant(moment) >= instant(last["event_at"]), "out_of_order", "transition precedes prior action")
            Catalog.device(db, row["device_id"], moment)
            next_revision = expected_revision + 1
            closed = moment if target == "closed" else None
            db.execute("UPDATE tickets SET state=?,closed_at=?,revision=? WHERE id=?", (target, closed, next_revision, ticket_id))
            db.execute("INSERT INTO ticket_history VALUES(?,?,?,?,?,?)", (ticket_id, next_revision, moment, action, target, note))
            source_revision = self.store.bump(db)
            self.store.audit(db, "ticket_transition", ticket_id, {"from": row["state"], "to": target, "source_revision": source_revision})
            return {"id": ticket_id, "state": target, "revision": next_revision, "closed_at": closed}

    def ticket(self, ticket_id: str) -> dict:
        with self.store.read() as db:
            row = db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            require(row is not None, "unknown_ticket", "ticket unknown", 404)
            history = db.execute("SELECT * FROM ticket_history WHERE ticket_id=? ORDER BY revision", (ticket_id,)).fetchall()
            return {**dict(row), "history": [dict(item) for item in history]}

    def list_for_device(self, device_id: str) -> list[dict]:
        with self.store.read() as db:
            Catalog.device(db, device_id)
            return [dict(row) for row in db.execute("SELECT t.* FROM tickets t LEFT JOIN ticket_revocations r ON r.ticket_id=t.id AND r.status='active' WHERE t.device_id=? AND r.ticket_id IS NULL ORDER BY t.opened_at,t.id", (device_id,))]
