"""跨市场设备服务质量分析：lineage模块。"""
import json
from .catalog import market_day
from .errors import require
from .publication import Publication
from .store import Store


class Lineage:
    def __init__(self, store: Store):
        self.store = store
        self.publication = Publication(store)

    def explain(self, market: str, version: int, day: str, product: str) -> dict:
        release = self.publication.read(market, version)
        metric = next((r for r in release["rows"] if r["day"] == day and r["product"] == product), None)
        require(metric is not None, "unknown_metric", "published metric unknown", 404)
        with self.store.read() as db:
            zone = db.execute("SELECT timezone FROM markets WHERE code=?", (market,)).fetchone()[0]
            devices = [dict(row) for row in db.execute("SELECT * FROM devices WHERE market=? AND product=? ORDER BY id", (market, product))]
            ids = [row["id"] for row in devices]
            source_rows = []
            for device in ids:
                ticket_rows = db.execute("SELECT t.*,r.revocation_id AS revoked_in,r.status AS revocation_status FROM tickets t LEFT JOIN ticket_revocations r ON r.ticket_id=t.id WHERE t.device_id=? ORDER BY t.opened_at,t.id", (device,)).fetchall()
                for row in ticket_rows:
                    if market_day(row["opened_at"], zone) == day:
                        source_rows.append({"source": "tickets", "id": row["id"], "device_id": device, "origin_batch": row["origin_batch"], "at": row["opened_at"], "revoked": row["revoked_in"] is not None, "revocation_active": row["revocation_status"] == "active"})
                meter_rows = db.execute("SELECT e.*,x.revocation_id AS revoked_in,x.status AS revocation_status FROM telemetry e LEFT JOIN telemetry_revocations x ON x.event_id=e.id WHERE e.device_id=? ORDER BY e.observed_at,e.id", (device,)).fetchall()
                for row in meter_rows:
                    if market_day(row["observed_at"], zone) == day:
                        source_rows.append({"source": "telemetry", "id": row["id"], "device_id": device, "origin_batch": row["origin_batch"], "at": row["observed_at"], "revoked": row["revoked_in"] is not None, "revocation_active": row["revocation_status"] == "active"})
            history = []
            for row in source_rows:
                if row["source"] != "tickets":
                    continue
                for item in db.execute("SELECT * FROM ticket_history WHERE ticket_id=? ORDER BY revision", (row["id"],)):
                    history.append(dict(item))
            return {"market": market, "version": version, "day": day, "product": product, "source_revision": release["source_revision"], "metric": metric, "devices": devices, "source_rows": source_rows, "ticket_history": history}

    def batch_trace(self, batch_id: str) -> dict:
        with self.store.read() as db:
            batch = db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            require(batch is not None, "unknown_batch", "batch unknown", 404)
            rows = [dict(r) for r in db.execute("SELECT * FROM batch_rows WHERE batch_id=? ORDER BY row_no", (batch_id,))]
            for row in rows:
                row["payload"] = json.loads(row["payload"])
            revocation = db.execute("SELECT * FROM revocations WHERE batch_id=?", (batch_id,)).fetchone()
            corrected_by = db.execute("SELECT id,request_key,status,created_at FROM batches WHERE corrects=? ORDER BY created_at", (batch_id,)).fetchall()
            return {"batch": dict(batch), "rows": rows, "revocation": dict(revocation) if revocation else None, "corrected_by": [dict(r) for r in corrected_by]}
