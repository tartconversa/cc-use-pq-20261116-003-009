import tempfile
import unittest
from pathlib import Path
from fastapi.testclient import TestClient
from brand_signal.api import create_app


class RevocationApiTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.client = TestClient(create_app(str(Path(directory.name) / "http.sqlite3")))
        self.client.post("/markets", json={"code": "ES", "timezone": "Europe/Madrid"})
        self.client.post("/products", json={"code": "dryer", "name": "Synthetic Dryer"})
        self.client.post("/devices", json={"id": "d1", "market": "ES", "product": "dryer", "installed_at": "2026-10-01T00:00:00Z"})
        self.ticket_row = {"event_key": "e1", "id": "t1", "device_id": "d1", "opened_at": "2026-10-20T22:30:00Z", "severity": "high"}

    def submit(self, key="r1", corrects=None):
        body = {"request_key": key, "rows": [self.ticket_row]}
        if corrects:
            body["corrects"] = corrects
        return self.client.post("/ingest/tickets", json=body)

    def test_revoke_then_corrected_replay_end_to_end(self):
        batch = self.submit().json()
        revoke = self.client.post(f"/batches/{batch['id']}/revoke", json={"reason_code": "wrong_device_mapping", "reason": "bad mapping", "idempotency_key": "k1"})
        self.assertEqual(revoke.status_code, 200)
        self.assertEqual(revoke.json()["revoked_count"], 1)
        # 幂等重放撤销请求。
        again = self.client.post(f"/batches/{batch['id']}/revoke", json={"reason_code": "wrong_device_mapping", "reason": "bad mapping", "idempotency_key": "k1"})
        self.assertTrue(again.json()["replay"])
        # 修正重放。
        self.ticket_row["severity"] = "critical"
        replay = self.submit(key="fixed", corrects=batch["id"])
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(replay.json()["accepted"], 1)
        self.assertEqual(replay.json()["corrects"], batch["id"])
        # 批次溯源能看到撤销与修正链。
        trace = self.client.get(f"/lineage/batch/{batch['id']}").json()
        self.assertIsNotNone(trace["revocation"])
        self.assertEqual(trace["corrected_by"][0]["id"], replay.json()["id"])
        # 撤销详情、工单视角可查。
        self.assertEqual(self.client.get(f"/batches/{batch['id']}/revocation").status_code, 200)
        self.assertEqual(self.client.get("/revocations/" + revoke.json()["id"]).status_code, 200)
        view = self.client.get("/tickets/t1/revocation").json()
        self.assertIn("reopen", [h["action"] for h in view["history"]])

    def test_replay_rejects_foreign_event_key(self):
        batch = self.submit().json()
        self.client.post(f"/batches/{batch['id']}/revoke", json={"reason_code": "x", "reason": "y", "idempotency_key": "k"})
        foreign = {"request_key": "bad", "corrects": batch["id"], "rows": [{"event_key": "foreign", "id": "t2", "device_id": "d1", "opened_at": "2026-10-20T22:30:00Z"}]}
        replay = self.client.post("/ingest/tickets", json=foreign)
        self.assertEqual(replay.json()["rows"][0]["reason"], "replay_event_not_in_revoked_batch")

    def test_revoke_unknown_batch_is_404_and_replay_without_revoke_is_409(self):
        batch = self.submit().json()
        missing = self.client.post("/batches/nope/revoke", json={"reason_code": "x", "reason": "y", "idempotency_key": "k"})
        self.assertEqual(missing.status_code, 404)
        premature = self.client.post("/ingest/tickets", json={"request_key": "p", "corrects": batch["id"], "rows": [self.ticket_row]})
        self.assertEqual(premature.status_code, 409)
        self.assertEqual(premature.json()["error"], "batch_not_revoked")

    def test_published_version_immutable_after_revoke(self):
        batch = self.submit().json()
        candidate = self.client.post("/candidates", json={"market": "ES", "from_day": "2026-10-21", "to_day": "2026-10-21", "expected_head": 0}).json()
        self.client.post(f"/candidates/{candidate['id']}/publish", json={"expected_head": 0, "expected_source_revision": candidate["source_revision"]})
        before = self.client.get("/publications/ES/1").json()
        self.client.post(f"/batches/{batch['id']}/revoke", json={"reason_code": "x", "reason": "y", "idempotency_key": "k"})
        after = self.client.get("/publications/ES/1").json()
        self.assertEqual(before["rows"], after["rows"])
