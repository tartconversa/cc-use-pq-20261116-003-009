"""跨市场设备服务质量分析：analytics模块。"""
from collections import defaultdict
from datetime import date, timedelta
from .catalog import Catalog, instant, market_day
from .errors import DomainError, require
from .store import Store


def days_between(first: str, last: str):
    try:
        start, stop = date.fromisoformat(first), date.fromisoformat(last)
    except (TypeError, ValueError) as exc:
        raise DomainError("invalid_day", "date must be YYYY-MM-DD") from exc
    require(start.isoformat() == first and stop.isoformat() == last, "invalid_day", "date must be YYYY-MM-DD")
    count = (stop - start).days
    require(0 <= count <= 365, "invalid_range", "date interval must be 1..366 days")
    for offset in range(count + 1):
        yield (start + timedelta(days=offset)).isoformat()


def overlap_hours(start, end, boundary_start, boundary_end) -> float:
    left = max(start, boundary_start)
    right = min(end, boundary_end)
    return max(0, (right - left).total_seconds() / 3600)


class Analytics:
    def __init__(self, store: Store):
        self.store = store

    def calculate(self, db, market: str, from_day: str, to_day: str) -> list[dict]:
        zone_row = db.execute("SELECT timezone FROM markets WHERE code=?", (market,)).fetchone()
        require(zone_row is not None, "unknown_market", "market unknown", 404)
        zone = zone_row["timezone"]
        days = list(days_between(from_day, to_day))
        devices = db.execute("SELECT * FROM devices WHERE market=? ORDER BY id", (market,)).fetchall()
        products = [row["code"] for row in db.execute("SELECT code FROM products ORDER BY code")]
        tickets = db.execute("SELECT t.* FROM tickets t JOIN devices d ON d.id=t.device_id LEFT JOIN ticket_revocations r ON r.ticket_id=t.id AND r.status='active' WHERE d.market=? AND r.ticket_id IS NULL ORDER BY t.id", (market,)).fetchall()
        history = db.execute("SELECT h.* FROM ticket_history h JOIN tickets t ON t.id=h.ticket_id JOIN devices d ON d.id=t.device_id LEFT JOIN ticket_revocations r ON r.ticket_id=t.id AND r.status='active' WHERE d.market=? AND r.ticket_id IS NULL ORDER BY h.ticket_id,h.revision", (market,)).fetchall()
        telemetry = db.execute("SELECT e.* FROM telemetry e JOIN devices d ON d.id=e.device_id LEFT JOIN telemetry_revocations x ON x.event_id=e.id AND x.status='active' WHERE d.market=? AND x.event_id IS NULL ORDER BY e.id", (market,)).fetchall()
        group = {}
        for day in days:
            for product in products:
                group[(day, product)] = {"day": day, "market": market, "product": product, "installed_devices": 0, "active_devices": 0, "exposure_hours": 0.0, "ticket_opened": 0, "ticket_closed": 0, "ticket_open_at_end": 0, "critical_opened": 0, "acknowledged": 0, "response_hours_total": 0.0, "telemetry_events": 0, "devices_reporting": 0, "run_hours": 0.0, "downtime_hours": 0.0, "telemetry_coverage": None, "tickets_per_100_devices": None, "downtime_share": None}
        device_map = {r["id"]: r for r in devices}
        reported = defaultdict(set)
        opened = defaultdict(list)
        transitions = defaultdict(list)
        for item in history:
            transitions[item["ticket_id"]].append(item)
        for item in tickets:
            opened[item["device_id"]].append(item)
        for day in days:
            day_start, day_end = Catalog.local_bounds(day, zone)
            for device in devices:
                key = day, device["product"]
                result = group[key]
                installed = instant(device["installed_at"])
                retired = instant(device["retired_at"]) if device["retired_at"] else None
                if installed < day_end:
                    result["installed_devices"] += 1
                active_end = min(day_end, retired) if retired else day_end
                exposure = overlap_hours(installed, active_end, day_start, day_end)
                if exposure:
                    result["active_devices"] += 1
                    result["exposure_hours"] += exposure
            for ticket in tickets:
                device = device_map[ticket["device_id"]]
                result = group[(day, device["product"])]
                if market_day(ticket["opened_at"], zone) == day:
                    result["ticket_opened"] += 1
                    if ticket["severity"] == "critical":
                        result["critical_opened"] += 1
                actions = transitions[ticket["id"]]
                for action in actions:
                    if market_day(action["event_at"], zone) != day:
                        continue
                    if action["action"] == "close":
                        result["ticket_closed"] += 1
                    if action["action"] == "acknowledge":
                        result["acknowledged"] += 1
                        result["response_hours_total"] += (instant(action["event_at"]) - instant(ticket["opened_at"])).total_seconds() / 3600
                latest = "open"
                for action in actions:
                    if instant(action["event_at"]) < day_end:
                        latest = action["state"]
                if instant(ticket["opened_at"]) < day_end and latest not in {"closed", "cancelled"}:
                    result["ticket_open_at_end"] += 1
            for event in telemetry:
                if market_day(event["observed_at"], zone) == day:
                    device = device_map[event["device_id"]]
                    result = group[(day, device["product"])]
                    result["telemetry_events"] += 1
                    result["run_hours"] += event["run_hours"]
                    result["downtime_hours"] += event["downtime_hours"]
                    reported[(day, device["product"])].add(event["device_id"])
        for key, result in group.items():
            result["devices_reporting"] = len(reported[key])
            if result["active_devices"]:
                result["telemetry_coverage"] = result["devices_reporting"] / result["active_devices"]
                result["tickets_per_100_devices"] = 100 * result["ticket_opened"] / result["active_devices"]
            denominator = result["run_hours"] + result["downtime_hours"]
            if denominator:
                result["downtime_share"] = result["downtime_hours"] / denominator
            result["exposure_hours"] = round(result["exposure_hours"], 6)
            result["response_hours_total"] = round(result["response_hours_total"], 6)
            result["run_hours"] = round(result["run_hours"], 6)
            result["downtime_hours"] = round(result["downtime_hours"], 6)
        return list(group.values())

    def live(self, market: str, from_day: str, to_day: str) -> dict:
        with self.store.read() as db:
            return {"source_revision": self.store.revision(db), "rows": self.calculate(db, market, from_day, to_day)}

    def quality(self, market: str) -> dict:
        with self.store.read() as db:
            require(db.execute("SELECT 1 FROM markets WHERE code=?", (market,)).fetchone(), "unknown_market", "market unknown", 404)
            sources = {}
            for source in ("tickets", "telemetry"):
                row = db.execute("SELECT count(*) batches,coalesce(sum(accepted),0) accepted,coalesce(sum(rejected),0) rejected FROM batches WHERE source=? AND status!='revoked'", (source,)).fetchone()
                sources[source] = dict(row)
            unknown = db.execute("SELECT count(*) FROM devices WHERE market=?", (market,)).fetchone()[0]
            return {"market": market, "devices": unknown, "sources": sources, "source_revision": self.store.revision(db)}

    def fleet_summary(self, market: str, from_day: str, to_day: str) -> dict:
        report = self.live(market, from_day, to_day)
        by_product = defaultdict(lambda: {"device_days": 0, "reporting_device_days": 0, "tickets": 0, "critical_tickets": 0, "closed": 0, "exposure_hours": 0.0, "run_hours": 0.0, "downtime_hours": 0.0})
        for row in report["rows"]:
            item = by_product[row["product"]]
            item["device_days"] += row["active_devices"]
            item["reporting_device_days"] += row["devices_reporting"]
            item["tickets"] += row["ticket_opened"]
            item["critical_tickets"] += row["critical_opened"]
            item["closed"] += row["ticket_closed"]
            item["exposure_hours"] += row["exposure_hours"]
            item["run_hours"] += row["run_hours"]
            item["downtime_hours"] += row["downtime_hours"]
        products = []
        for product, item in sorted(by_product.items()):
            observed = item["run_hours"] + item["downtime_hours"]
            products.append({"product": product, **item, "ticket_rate_per_100_device_days": 100 * item["tickets"] / item["device_days"] if item["device_days"] else None, "coverage": item["reporting_device_days"] / item["device_days"] if item["device_days"] else None, "downtime_share": item["downtime_hours"] / observed if observed else None})
        return {"market": market, "from_day": from_day, "to_day": to_day, "source_revision": report["source_revision"], "products": products}

    def device_timeline(self, device_id: str, from_day: str, to_day: str) -> dict:
        days = set(days_between(from_day, to_day))
        with self.store.read() as db:
            device = Catalog.device(db, device_id)
            zone = device["timezone"]
            timeline = {day: {"day": day, "device_id": device_id, "tickets_opened": [], "ticket_actions": [], "telemetry": []} for day in sorted(days)}
            for row in db.execute("SELECT t.* FROM tickets t LEFT JOIN ticket_revocations r ON r.ticket_id=t.id AND r.status='active' WHERE t.device_id=? AND r.ticket_id IS NULL ORDER BY t.opened_at,t.id", (device_id,)):
                day = market_day(row["opened_at"], zone)
                if day in timeline:
                    timeline[day]["tickets_opened"].append(row["id"])
                for action in db.execute("SELECT h.* FROM ticket_history h WHERE h.ticket_id=? ORDER BY h.revision", (row["id"],)):
                    action_day = market_day(action["event_at"], zone)
                    if action_day in timeline:
                        timeline[action_day]["ticket_actions"].append({"ticket_id": row["id"], "action": action["action"], "revision": action["revision"]})
            for row in db.execute("SELECT e.* FROM telemetry e LEFT JOIN telemetry_revocations x ON x.event_id=e.id AND x.status='active' WHERE e.device_id=? AND x.event_id IS NULL ORDER BY e.observed_at,e.id", (device_id,)):
                day = market_day(row["observed_at"], zone)
                if day in timeline:
                    timeline[day]["telemetry"].append({"event_id": row["id"], "run_hours": row["run_hours"], "downtime_hours": row["downtime_hours"]})
            return {"device": dict(device), "source_revision": self.store.revision(db), "days": list(timeline.values())}
