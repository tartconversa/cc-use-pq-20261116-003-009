"""跨市场设备服务质量分析：telemetry模块。"""
import math
from .catalog import Catalog, iso
from .errors import DomainError, require
from .store import Store


class Telemetry:
    def __init__(self, store: Store):
        self.store = store

    def validate(self, db, event: dict, supersede_revoked: bool = False) -> dict:
        require(isinstance(event, dict), "invalid_event", "event object required")
        event_id = event.get("id")
        device_id = event.get("device_id")
        require(isinstance(event_id, str) and 1 <= len(event_id) <= 100, "invalid_event_id", "event id required")
        require(isinstance(device_id, str), "invalid_device", "device id required")
        observed = iso(event.get("observed_at"))
        Catalog.device(db, device_id, observed)
        hours = event.get("run_hours")
        downtime = event.get("downtime_hours")
        require(type(hours) in (int, float) and math.isfinite(hours) and 0 <= hours <= 24, "invalid_hours", "run hours must be within a day")
        require(type(downtime) in (int, float) and math.isfinite(downtime) and 0 <= downtime <= 24, "invalid_hours", "downtime hours must be within a day")
        require(hours + downtime <= 24, "invalid_hours", "combined hours exceed a day")
        existing = db.execute("SELECT event_id FROM telemetry_revocations WHERE event_id=?", (event_id,)).fetchone()
        active = db.execute("SELECT 1 FROM telemetry WHERE id=?", (event_id,)).fetchone()
        if active is not None:
            require(existing is not None and supersede_revoked, "event_exists", "telemetry id exists", 409)
        return {"id": event_id, "device_id": device_id, "observed_at": observed, "run_hours": float(hours), "downtime_hours": float(downtime)}

    def accept(self, db, event: dict, batch_id: str, supersede_revoked: bool = False) -> dict:
        data = self.validate(db, event, supersede_revoked)
        if supersede_revoked and db.execute("SELECT 1 FROM telemetry WHERE id=?", (data["id"],)).fetchone():
            # 修正重放：更新已撤销计量记录为有效来源的新值，原始值已在补偿表留档。
            db.execute(
                "UPDATE telemetry SET device_id=?,observed_at=?,run_hours=?,downtime_hours=?,origin_batch=? WHERE id=?",
                (data["device_id"], data["observed_at"], data["run_hours"], data["downtime_hours"], batch_id, data["id"]),
            )
            db.execute("UPDATE telemetry_revocations SET status='superseded' WHERE event_id=?", (data["id"],))
            self.store.bump(db)
            self.store.audit(db, "telemetry_replayed", data["id"], {"device": data["device_id"], "batch": batch_id})
            return data
        db.execute("INSERT INTO telemetry VALUES(?,?,?,?,?,?)", (data["id"], data["device_id"], data["observed_at"], data["run_hours"], data["downtime_hours"], batch_id))
        self.store.bump(db)
        self.store.audit(db, "telemetry_accepted", data["id"], {"device": data["device_id"], "batch": batch_id})
        return data

    def list_for_device(self, device_id: str) -> list[dict]:
        with self.store.read() as db:
            Catalog.device(db, device_id)
            return [dict(row) for row in db.execute("SELECT e.* FROM telemetry e LEFT JOIN telemetry_revocations x ON x.event_id=e.id AND x.status='active' WHERE e.device_id=? AND x.event_id IS NULL ORDER BY e.observed_at,e.id", (device_id,))]
