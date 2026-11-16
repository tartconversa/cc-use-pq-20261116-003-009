import threading
from brand_signal.errors import DomainError
from brand_signal.revocations import Revocations
from brand_signal.lineage import Lineage
from brand_signal.contracts import Contracts
from support import Scenario


class RevocationTests(Scenario):
    def revoke(self, batch_id, reason="wrong_device_mapping", key="idem-1"):
        return Revocations(self.store).revoke(batch_id, reason, "batch arrived under an incorrect device mapping", key)

    # ------------------------------------------------------------ 基础撤销
    def test_revoke_keeps_batch_and_marks_rows_revoked(self):
        batch = self.submit_ticket()
        result = self.revoke(batch["id"])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["revoked_count"], 1)
        # 原始批次记录仍在，但接受行被标记为 revoked 并带逐行原因。
        stored = self.intake.batch(batch["id"])
        self.assertEqual(stored["status"], "revoked")
        self.assertEqual(stored["rows"][0]["status"], "revoked")
        self.assertEqual(stored["rows"][0]["reason"], "wrong_device_mapping")
        # 原始逐行 payload 仍可追溯。
        trace = Lineage(self.store).batch_trace(batch["id"])
        self.assertEqual(trace["rows"][0]["payload"]["id"], "ticket-1")
        self.assertIsNotNone(trace["revocation"])

    def test_revoke_releases_event_key_and_removes_live_metric(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        # 事件键占用已释放：原键不再出现在 source_events。
        with self.store.read() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM source_events WHERE source='tickets' AND event_key='event-1'").fetchone())
            archived = db.execute("SELECT * FROM event_revocations WHERE event_key='event-1'").fetchone()
            self.assertEqual(archived["original_batch"], batch["id"])
        # 当前工单指标不再计入该来源。
        live = self.analytics.live("ES", "2026-10-20", "2026-10-21")["rows"]
        self.assertEqual(sum(r["ticket_opened"] for r in live), 0)
        # 工单记录仍保留，状态为 revoked。
        ticket = self.service.ticket("ticket-1")
        self.assertEqual(ticket["state"], "revoked")
        self.assertIn("revoke", [h["action"] for h in ticket["history"]])

    def test_revoked_ticket_excluded_from_device_listing_but_history_kept(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        self.assertEqual(self.service.list_for_device("device-1"), [])
        ticket = self.service.ticket("ticket-1")
        self.assertTrue(any(h["action"] == "revoke" for h in ticket["history"]))

    def test_revoked_telemetry_excluded_from_metrics(self):
        batch = self.submit_meter()
        before = self.analytics.live("ES", "2026-10-21", "2026-10-21")["rows"][0]
        self.assertEqual(before["telemetry_events"], 1)
        self.revoke(batch["id"])
        after = self.analytics.live("ES", "2026-10-21", "2026-10-21")["rows"][0]
        self.assertEqual(after["telemetry_events"], 0)
        self.assertEqual(after["run_hours"], 0.0)
        self.assertEqual(self.telemetry.list_for_device("device-1"), [])

    def test_unknown_batch_and_double_revoke_are_rejected(self):
        batch = self.submit_ticket()
        with self.assertRaises(DomainError) as error:
            self.revoke("missing", key="x")
        self.assertEqual(error.exception.code, "unknown_batch")
        self.revoke(batch["id"], key="first")
        with self.assertRaises(DomainError) as error:
            self.revoke(batch["id"], key="second")
        self.assertEqual(error.exception.code, "batch_already_revoked")

    # ------------------------------------------------------------ 幂等与并发
    def test_revoke_is_idempotent_by_key(self):
        batch = self.submit_ticket()
        first = self.revoke(batch["id"], key="same")
        second = self.revoke(batch["id"], key="same")
        self.assertEqual(first["id"], second["id"])
        self.assertFalse(first["replay"])
        self.assertTrue(second["replay"])
        # 没有双重扣除：只有一条撤销、一条补偿。
        with self.store.read() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM revocations").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM record_compensations").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM ticket_revocations").fetchone()[0], 1)

    def test_concurrent_revocations_do_not_double_deduct(self):
        batch = self.submit_ticket()
        outcomes = []

        def worker(key):
            try:
                outcomes.append(Revocations(self.store).revoke(batch["id"], "race", "concurrent", key))
            except DomainError as exc:
                outcomes.append(exc.code)

        threads = [threading.Thread(target=worker, args=(f"key-{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 恰好一个成功撤销，其余因批次已撤销被拒，记录只被扣一次。
        succeeded = [o for o in outcomes if isinstance(o, dict)]
        rejected = [o for o in outcomes if o == "batch_already_revoked"]
        self.assertEqual(len(succeeded), 1)
        self.assertEqual(len(rejected), 3)
        with self.store.read() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM revocations").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM event_revocations").fetchone()[0], 1)

    # ------------------------------------------------------------ 后续状态关联
    def test_followup_state_history_is_kept_and_linked_to_compensation(self):
        batch = self.submit_ticket()
        self.service.transition("ticket-1", "acknowledge", "2026-10-21T00:30:00Z", 1)
        self.service.transition("ticket-1", "resolve", "2026-10-21T02:00:00Z", 2)
        revocation = self.revoke(batch["id"])
        link = revocation["ticket_links"][0]
        self.assertTrue(link["had_followup"])
        self.assertEqual(link["final_state"], "resolved")
        # 工单视角：补偿记录与全部历史（含撤销前的变迁）通过 ticket_id 关联。
        view = Revocations(self.store).list_for_ticket("ticket-1")
        actions = [h["action"] for h in view["history"]]
        self.assertEqual(actions, ["open", "acknowledge", "resolve", "revoke"])
        self.assertEqual(view["compensations"][0]["direction"], "reverse")
        self.assertTrue(view["compensations"][0]["payload"]["had_followup"])

    def test_transition_after_revoke_is_blocked(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        with self.assertRaises(DomainError) as error:
            self.service.transition("ticket-1", "acknowledge", "2026-10-21T00:30:00Z", 2)
        self.assertEqual(error.exception.code, "ticket_revoked")

    # ------------------------------------------------------------ 修正重放
    def test_corrected_replay_reuses_event_key_without_conflict(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        corrected = dict(self.ticket(device="device-1", severity="critical"))
        replay = self.intake.submit("tickets", "fixed-1", [corrected], corrects=batch["id"])
        self.assertEqual(replay["accepted"], 1)
        self.assertEqual(replay["corrects"], batch["id"])
        # 事件键重新占用，指向有效来源记录。
        trace = Contracts(self.store).trace("tickets", "event-1")
        self.assertEqual(trace["batch_id"], replay["id"])
        ticket = self.service.ticket("ticket-1")
        self.assertEqual(ticket["state"], "open")
        self.assertEqual(ticket["severity"], "critical")
        # 当前指标重新计入有效来源且只计一次。
        live = self.analytics.live("ES", "2026-10-20", "2026-10-21")["rows"]
        self.assertEqual(sum(r["ticket_opened"] for r in live), 1)
        self.assertEqual(sum(r["critical_opened"] for r in live), 1)

    def test_replay_rejects_keys_not_in_revoked_batch_per_row(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        foreign = self.ticket(key="new-key", identifier="ticket-2")
        replay = self.intake.submit("tickets", "fixed-2", [foreign], corrects=batch["id"])
        self.assertEqual(replay["accepted"], 0)
        self.assertEqual(replay["rows"][0]["reason"], "replay_event_not_in_revoked_batch")

    def test_replay_requires_prior_revocation(self):
        batch = self.submit_ticket()
        with self.assertRaises(DomainError) as error:
            self.intake.submit("tickets", "premature", [self.ticket()], corrects=batch["id"])
        self.assertEqual(error.exception.code, "batch_not_revoked")

    def test_replay_request_key_replay_is_idempotent_no_double_count(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        rows = [dict(self.ticket(severity="critical"))]
        first = self.intake.submit("tickets", "stable-fix", rows, corrects=batch["id"])
        second = self.intake.submit("tickets", "stable-fix", rows, corrects=batch["id"])
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["replay"])
        live = self.analytics.live("ES", "2026-10-20", "2026-10-21")["rows"]
        self.assertEqual(sum(r["ticket_opened"] for r in live), 1)
        with self.store.read() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM tickets WHERE state!='revoked'").fetchone()[0], 1)

    def test_replay_row_can_still_be_rejected_for_business_rule(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        bad = dict(self.ticket(device="absent"))
        replay = self.intake.submit("tickets", "fixed-bad", [bad], corrects=batch["id"])
        self.assertEqual(replay["rows"][0]["reason"], "unknown_device")
        self.assertEqual(replay["accepted"], 0)

    def test_correction_batch_itself_cannot_be_revoked(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        replay = self.intake.submit("tickets", "fixed", [dict(self.ticket())], corrects=batch["id"])
        with self.assertRaises(DomainError) as error:
            self.revoke(replay["id"], key="nested")
        self.assertEqual(error.exception.code, "correction_batch_not_revocable")

    def test_replay_through_corrected_device_mapping(self):
        # 原批次经错误映射进入 device-1；撤销后修正外部号映射，重放落到正确设备。
        self.catalog.install("device-2", "ES", "dryer", "2026-10-01T00:00:00Z")
        from brand_signal.mappings import Mappings
        mappings = Mappings(self.store)
        # 错误映射在 10-22 失效，之后同一外部号改指向正确设备。
        mappings.register("telemetry", "serial-x", "device-1", "2026-10-01T00:00:00Z", "2026-10-22T00:00:00Z")
        row = self.meter()
        del row["device_id"]
        row["external_device_id"] = "serial-x"
        batch = self.intake.submit("telemetry", "wrong-map", [row])
        self.revoke(batch["id"])
        # 修正映射后，同一外部号解析到 device-2。
        mappings.register("telemetry", "serial-x", "device-2", "2026-10-22T00:00:00Z")
        corrected_row = dict(row)
        corrected_row["observed_at"] = "2026-10-22T08:00:00Z"
        replay = self.intake.submit("telemetry", "right-map", [corrected_row], corrects=batch["id"])
        self.assertEqual(replay["accepted"], 1)
        meters = self.telemetry.list_for_device("device-2")
        self.assertEqual(len(meters), 1)
        self.assertEqual(self.telemetry.list_for_device("device-1"), [])

    # ------------------------------------------------------- 发布与候选语义
    def test_unpublished_candidate_is_invalidated_but_published_version_immutable(self):
        batch = self.submit_ticket()
        # 先发布 v1，已发布版本必须不受撤销影响。
        v1 = self.release("2026-10-21")
        frozen_before = self.publication.read("ES", v1["version"])["rows"]
        # 准备一个尚未发布的候选，然后撤销来源批次。
        pending = self.publication.prepare("ES", "2026-10-21", "2026-10-21", 1)
        self.assertTrue(self.publication.candidate(pending["id"])["ready"])
        self.revoke(batch["id"])
        stale = self.publication.candidate(pending["id"])
        self.assertFalse(stale["ready"])
        self.assertIn("source_revision_superseded", stale["invalid_reasons"])
        # 尝试发布失效候选被拒。
        with self.assertRaises(DomainError) as error:
            self.publication.publish(pending["id"], 1, pending["source_revision"])
        self.assertEqual(error.exception.code, "stale_source")
        # 已发布 v1 完全不变。
        frozen_after = self.publication.read("ES", v1["version"])["rows"]
        self.assertEqual(frozen_before, frozen_after)

    def test_new_publication_after_replay_excludes_revoked_and_counts_valid_source(self):
        self.release("2026-10-21")  # v1 含错误来源
        batch = self.submit_ticket(at="2026-10-21T08:00:00Z")
        self.revoke(batch["id"])
        self.intake.submit("tickets", "fix", [dict(self.ticket(at="2026-10-21T08:00:00Z", severity="critical"))], corrects=batch["id"])
        candidate = self.publication.prepare("ES", "2026-10-21", "2026-10-21", 1)
        v2 = self.publication.publish(candidate["id"], 1, candidate["source_revision"])
        rows = self.publication.read("ES", v2["version"])["rows"][0]
        self.assertEqual(rows["ticket_opened"], 1)
        self.assertEqual(rows["critical_opened"], 1)
        # v1 与 v2 的差异可追溯，v1 仍保留错误来源当时的计数。
        delta = self.publication.diff("ES", 1, 2)["changes"]
        self.assertTrue(delta)

    def test_lineage_marks_revoked_sources_but_keeps_published_metric(self):
        batch = self.submit_ticket()
        release = self.release("2026-10-21")
        self.revoke(batch["id"])
        explanation = Lineage(self.store).explain("ES", release["version"], "2026-10-21", "dryer")
        self.assertEqual(explanation["metric"]["ticket_opened"], 1)  # 发布指标不变
        source = next(r for r in explanation["source_rows"] if r["source"] == "tickets")
        self.assertTrue(source["revoked"])  # 来源被标注为事后撤销

    def test_quality_summary_excludes_revoked_batch(self):
        batch = self.submit_ticket()
        self.revoke(batch["id"])
        quality = self.analytics.quality("ES")
        self.assertEqual(quality["sources"]["tickets"]["batches"], 0)
        self.assertEqual(quality["sources"]["tickets"]["accepted"], 0)

    def test_revocation_advances_source_revision(self):
        self.submit_ticket()
        before = self.analytics.live("ES", "2026-10-21", "2026-10-21")["source_revision"]
        batch = self.intake.batch  # noqa: F841 (no-op to keep readability)
        # 再提交一批并撤销，修订号应单调增加。
        second = self.submit_meter()
        mid = self.analytics.live("ES", "2026-10-21", "2026-10-21")["source_revision"]
        self.assertGreater(mid, before)
        self.revoke(second["id"], key="meter-revoke")
        after = self.analytics.live("ES", "2026-10-21", "2026-10-21")["source_revision"]
        self.assertGreater(after, mid)
