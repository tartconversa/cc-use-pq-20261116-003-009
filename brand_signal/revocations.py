"""跨市场设备服务质量分析：批次撤销与修正重放模块。

设计原则：
- 软撤销而非硬删：原始批次、逐行 payload 与接收记录永久保留，仅把接受行标记为 revoked。
- 补偿留痕：每条被撤销的业务记录写入反向补偿与逐行撤销原因，工单保留完整状态历史。
- 事件键释放：source_events 中占用被移除并转存 event_revocations，使修正重放不再键冲突，
  同时保留原摘要、原批次与撤销理由，支持完整来历追溯。
- 版本语义：撤销与重放均推进全局 source revision；未发布候选因修订失效而不可发布，
  已发布版本与导出不可变。
- 幂等与并发：撤销以 idempotency_key 去重并在 BEGIN IMMEDIATE 事务内串行化，
  重复撤销返回同一结果；重放复用 request_key 幂等，杜绝双重扣除与重复计数。
"""
import json
import uuid
from .errors import DomainError, require
from .store import Store, now


REVOKE_REASON = "source_revoked"
REPLAY_FOREIGN_KEY = "replay_foreign_key"
REPLAY_NOT_CORRECTED = "replay_event_not_in_revoked_batch"


class Revocations:
    def __init__(self, store: Store):
        self.store = store

    # ------------------------------------------------------------------ 撤销
    def revoke(self, batch_id: str, reason_code: str, reason: str, idempotency_key: str) -> dict:
        require(isinstance(reason_code, str) and 1 <= len(reason_code) <= 60, "invalid_reason", "reason code required")
        require(isinstance(reason, str) and 1 <= len(reason.strip()) <= 1000, "invalid_reason", "reason text required")
        require(isinstance(idempotency_key, str) and 1 <= len(idempotency_key) <= 100, "invalid_idempotency_key", "idempotency key required")
        with self.store.transaction() as db:
            # 并发撤销：唯一 idempotency_key 保证只有一个事务生效，其余回退为幂等重放。
            existing = db.execute("SELECT * FROM revocations WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if existing:
                return self._result(db, existing["id"], replay=True)
            batch = db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            require(batch is not None, "unknown_batch", "batch unknown", 404)
            prior = db.execute("SELECT id FROM revocations WHERE batch_id=?", (batch_id,)).fetchone()
            require(prior is None, "batch_already_revoked", "batch already revoked", 409)
            require(batch["corrects"] is None, "correction_batch_not_revocable", "correction replay batch cannot itself be revoked", 409)
            source = batch["source"]
            accepted_rows = db.execute(
                "SELECT * FROM batch_rows WHERE batch_id=? AND status='accepted' ORDER BY row_no",
                (batch_id,),
            ).fetchall()
            revocation_id = uuid.uuid4().hex
            stamp = now()
            accepted_count = len(accepted_rows)
            # 先落撤销主记录，使逐行 event_revocations 的外键成立；完成后再置 completed。
            db.execute(
                "INSERT INTO revocations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (revocation_id, batch_id, source, reason_code, reason.strip(), idempotency_key,
                 "processing", accepted_count, self.store.revision(db), stamp),
            )
            revoked_records = []
            for row in accepted_rows:
                event = db.execute(
                    "SELECT * FROM source_events WHERE source=? AND event_key=?",
                    (source, row["event_key"]),
                ).fetchone()
                require(event is not None, "event_state_missing", "accepted event has no source registration", 409)
                record_id = event["record_id"]
                # 逐行撤销原因与原始指纹留档，然后释放事件键占用。
                db.execute(
                    "INSERT INTO event_revocations VALUES(?,?,?,?,?,?,?,?)",
                    (source, row["event_key"], record_id, batch_id, revocation_id, event["digest"], reason_code, stamp),
                )
                db.execute(
                    "DELETE FROM source_events WHERE source=? AND event_key=?",
                    (source, row["event_key"]),
                )
                db.execute(
                    "UPDATE batch_rows SET status='revoked', reason=? WHERE batch_id=? AND row_no=?",
                    (reason_code, batch_id, row["row_no"]),
                )
                revoked_records.append({"row_no": row["row_no"], "event_key": row["event_key"], "record_id": record_id})
                if source == "tickets":
                    self._revoke_ticket(db, record_id, batch_id, revocation_id, reason_code, stamp)
                else:
                    self._revoke_telemetry(db, record_id, batch_id, revocation_id, reason_code, stamp)
            accepted_count = len(accepted_rows)
            rejected_count = db.execute(
                "SELECT count(*) FROM batch_rows WHERE batch_id=? AND status='rejected'",
                (batch_id,),
            ).fetchone()[0]
            db.execute(
                "UPDATE batches SET status='revoked', accepted=0, rejected=? WHERE id=?",
                (rejected_count + accepted_count, batch_id),
            )
            source_revision = self.store.bump(db)
            db.execute(
                "UPDATE revocations SET status='completed', source_revision=? WHERE id=?",
                (source_revision, revocation_id),
            )
            self.store.audit(db, "batch_revoked", batch_id, {
                "revocation": revocation_id,
                "source": source,
                "reason_code": reason_code,
                "revoked": accepted_count,
                "source_revision": source_revision,
            })
            return self._result(db, revocation_id, replay=False)

    def _revoke_ticket(self, db, ticket_id: str, batch_id: str, revocation_id: str, reason_code: str, stamp: str) -> None:
        ticket = db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        require(ticket is not None, "event_state_missing", "ticket record missing", 409)
        followup = db.execute(
            "SELECT count(*) FROM ticket_history WHERE ticket_id=? AND revision>1",
            (ticket_id,),
        ).fetchone()[0]
        had_followup = 1 if followup else 0
        final_state = ticket["state"]
        # 工单不删除：置 revoked，全部状态变迁历史原样保留，补偿记录与历史通过 ticket_id 关联。
        db.execute("UPDATE tickets SET state='revoked', revision=revision+1 WHERE id=?", (ticket_id,))
        db.execute(
            "INSERT INTO ticket_history VALUES(?,?,?,?,?,?)",
            (ticket_id, ticket["revision"] + 1, stamp, "revoke", "revoked", reason_code),
        )
        db.execute(
            "INSERT INTO ticket_revocations(ticket_id,revocation_id,origin_batch,revoke_batch,device_id,source_revision,created_at,had_followup,final_state,status) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (ticket_id, revocation_id, batch_id, batch_id, ticket["device_id"],
             self.store.revision(db), stamp, had_followup, final_state, "active"),
        )
        db.execute(
            "INSERT INTO record_compensations VALUES(?,?,?,?,?,?)",
            (ticket_id, "ticket", revocation_id, "reverse",
             json.dumps({"id": ticket_id, "device_id": ticket["device_id"], "opened_at": ticket["opened_at"],
                         "severity": ticket["severity"], "prior_state": final_state, "had_followup": bool(had_followup)},
                        sort_keys=True, ensure_ascii=False),
             stamp),
        )
        self.store.audit(db, "ticket_revoked", ticket_id, {
            "revocation": revocation_id,
            "origin_batch": batch_id,
            "had_followup": bool(had_followup),
            "final_state": final_state,
        })

    def _revoke_telemetry(self, db, event_id: str, batch_id: str, revocation_id: str, reason_code: str, stamp: str) -> None:
        event = db.execute("SELECT * FROM telemetry WHERE id=?", (event_id,)).fetchone()
        require(event is not None, "event_state_missing", "telemetry record missing", 409)
        db.execute(
            "INSERT INTO telemetry_revocations(event_id,revocation_id,origin_batch,revoke_batch,device_id,source_revision,created_at,status) VALUES(?,?,?,?,?,?,?,?)",
            (event_id, revocation_id, batch_id, batch_id, event["device_id"],
             self.store.revision(db), stamp, "active"),
        )
        db.execute(
            "INSERT INTO record_compensations VALUES(?,?,?,?,?,?)",
            (event_id, "telemetry", revocation_id, "reverse",
             json.dumps({"id": event_id, "device_id": event["device_id"], "observed_at": event["observed_at"],
                         "run_hours": event["run_hours"], "downtime_hours": event["downtime_hours"]},
                        sort_keys=True, ensure_ascii=False),
             stamp),
        )
        self.store.audit(db, "telemetry_revoked", event_id, {
            "revocation": revocation_id,
            "origin_batch": batch_id,
        })

    # ------------------------------------------------------------ 修正重放
    def corrected_keys(self, db, original_batch_id: str) -> set[str]:
        keys = set()
        for row in db.execute(
            "SELECT event_key FROM event_revocations WHERE original_batch=?",
            (original_batch_id,),
        ).fetchall():
            keys.add(row["event_key"])
        return keys

    def validate_replay_row(self, db, source: str, row: dict, original_batch_id: str) -> None:
        """修正重放行校验：只允许重放被撤销批次中的原事件键，其它键逐行拒收。"""
        key = row.get("event_key") if isinstance(row, dict) else None
        require(isinstance(key, str) and 1 <= len(key) <= 100, "invalid_event_key", "event key required")
        allowed = self.corrected_keys(db, original_batch_id)
        require(key in allowed, REPLAY_NOT_CORRECTED, "event key was not part of the revoked batch")

    # ------------------------------------------------------------------ 查询
    @staticmethod
    def _result(db, revocation_id: str, replay: bool) -> dict:
        header = db.execute("SELECT * FROM revocations WHERE id=?", (revocation_id,)).fetchone()
        require(header is not None, "unknown_revocation", "revocation unknown", 404)
        rows = [dict(r) for r in db.execute(
            "SELECT source,event_key,record_id,original_batch,reason_code,created_at FROM event_revocations WHERE revocation_id=? ORDER BY event_key",
            (revocation_id,),
        )]
        compensations = [
            {**dict(r), "payload": json.loads(r["payload"])}
            for r in db.execute(
                "SELECT record_id,kind,direction,payload FROM record_compensations WHERE revocation_id=? ORDER BY kind,record_id",
                (revocation_id,),
            )
        ]
        ticket_links = [dict(r) for r in db.execute(
            "SELECT ticket_id,origin_batch,revoke_batch,had_followup,final_state,source_revision FROM ticket_revocations WHERE revocation_id=? ORDER BY ticket_id",
            (revocation_id,),
        )]
        meter_links = [dict(r) for r in db.execute(
            "SELECT event_id,origin_batch,revoke_batch,source_revision FROM telemetry_revocations WHERE revocation_id=? ORDER BY event_id",
            (revocation_id,),
        )]
        return {
            **dict(header),
            "replay": replay,
            "events": rows,
            "compensations": compensations,
            "ticket_links": ticket_links,
            "telemetry_links": meter_links,
        }

    def get(self, revocation_id: str) -> dict:
        with self.store.read() as db:
            return self._result(db, revocation_id, replay=False)

    def get_for_batch(self, batch_id: str) -> dict:
        with self.store.read() as db:
            require(db.execute("SELECT 1 FROM batches WHERE id=?", (batch_id,)).fetchone(),
                    "unknown_batch", "batch unknown", 404)
            row = db.execute("SELECT id FROM revocations WHERE batch_id=?", (batch_id,)).fetchone()
            require(row is not None, "batch_not_revoked", "batch has no revocation", 404)
            return self._result(db, row["id"], replay=False)

    def list_for_ticket(self, ticket_id: str) -> dict:
        """工单视角：撤销补偿与其后续状态历史如何关联。"""
        with self.store.read() as db:
            link = db.execute("SELECT * FROM ticket_revocations WHERE ticket_id=?", (ticket_id,)).fetchone()
            require(link is not None, "ticket_not_revoked", "ticket has no revocation", 404)
            history = [dict(r) for r in db.execute(
                "SELECT * FROM ticket_history WHERE ticket_id=? ORDER BY revision",
                (ticket_id,),
            )]
            compensations = [
                {**dict(r), "payload": json.loads(r["payload"])}
                for r in db.execute(
                    "SELECT kind,direction,payload,created_at FROM record_compensations WHERE record_id=? ORDER BY created_at",
                    (ticket_id,),
                )
            ]
            return {"ticket_id": ticket_id, "link": dict(link), "history": history, "compensations": compensations}
