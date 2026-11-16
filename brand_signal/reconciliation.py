"""跨市场设备服务质量分析：reconciliation模块。"""
import json
import uuid
from collections import defaultdict
from .analytics import Analytics, days_between
from .catalog import market_day
from .errors import require
from .store import Store, now


class Reconciliation:
    def __init__(self, store: Store):
        self.store = store
        self.analytics = Analytics(store)

    def inspect(self, market: str, from_day: str, to_day: str) -> dict:
        with self.store.read() as db:
            zone = db.execute("SELECT timezone FROM markets WHERE code=?", (market,)).fetchone()
            require(zone is not None, "unknown_market", "market unknown", 404)
            valid_days = set(days_between(from_day, to_day))
            metrics = self.analytics.calculate(db, market, from_day, to_day)
            issues = []
            devices = {r["id"]: r for r in db.execute("SELECT * FROM devices WHERE market=?", (market,))}
            meters = defaultdict(list)
            for row in db.execute("SELECT e.* FROM telemetry e JOIN devices d ON d.id=e.device_id LEFT JOIN telemetry_revocations x ON x.event_id=e.id AND x.status='active' WHERE d.market=? AND x.event_id IS NULL", (market,)):
                local = market_day(row["observed_at"], zone[0])
                if local in valid_days:
                    meters[(row["device_id"], local)].append(row)
            cases = defaultdict(list)
            for row in db.execute("SELECT t.* FROM tickets t JOIN devices d ON d.id=t.device_id LEFT JOIN ticket_revocations r ON r.ticket_id=t.id AND r.status='active' WHERE d.market=? AND r.ticket_id IS NULL", (market,)):
                local = market_day(row["opened_at"], zone[0])
                if local in valid_days:
                    cases[(row["device_id"], local)].append(row)
            for device in devices.values():
                for day in sorted(valid_days):
                    key = (device["id"], day)
                    readings = meters[key]
                    opened = cases[key]
                    if len(readings) > 1:
                        issues.append({"kind": "multiple_meter_events", "device": device["id"], "day": day, "events": [r["id"] for r in readings]})
                    if opened and not readings:
                        issues.append({"kind": "ticket_without_meter", "device": device["id"], "day": day, "tickets": [r["id"] for r in opened]})
                    for meter in readings:
                        if meter["downtime_hours"] > 0 and not opened:
                            issues.append({"kind": "downtime_without_ticket", "device": device["id"], "day": day, "event": meter["id"]})
            for row in metrics:
                if row["active_devices"] and not row["devices_reporting"]:
                    issues.append({"kind": "market_meter_gap", "product": row["product"], "day": row["day"], "active_devices": row["active_devices"]})
                if row["ticket_open_at_end"] > row["active_devices"]:
                    issues.append({"kind": "case_load_exceeds_fleet", "product": row["product"], "day": row["day"], "open_cases": row["ticket_open_at_end"]})
            issues.sort(key=lambda item: (item["day"], item["kind"], item.get("device", "")))
            return {"market": market, "from_day": from_day, "to_day": to_day, "source_revision": self.store.revision(db), "issues": issues, "issue_count": len(issues)}

    def save(self, market: str, from_day: str, to_day: str, expected_source_revision: int) -> dict:
        report = self.inspect(market, from_day, to_day)
        require(report["source_revision"] == expected_source_revision, "stale_source", "source revision changed", 409)
        identifier = uuid.uuid4().hex
        with self.store.transaction() as db:
            require(self.store.revision(db) == expected_source_revision, "stale_source", "source revision changed", 409)
            db.execute("INSERT INTO reconciliations VALUES(?,?,?,?,?,?,?)", (identifier, market, from_day, to_day, expected_source_revision, json.dumps(report, sort_keys=True), now()))
            self.store.audit(db, "reconciled", identifier, {"market": market, "source_revision": expected_source_revision, "issues": report["issue_count"]})
        return {"id": identifier, **report}

    def read(self, identifier: str) -> dict:
        with self.store.read() as db:
            row = db.execute("SELECT * FROM reconciliations WHERE id=?", (identifier,)).fetchone()
            require(row is not None, "unknown_reconciliation", "reconciliation unknown", 404)
            return {"id": identifier, **json.loads(row["payload"]), "created_at": row["created_at"]}
