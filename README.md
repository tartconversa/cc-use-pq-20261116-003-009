# 跨市场设备服务质量数据平台

该 FastAPI 服务在离线环境下分析不同市场的设备运维事件。演示数据均为本地合成数据，SQLite 是唯一持久化服务。

## 启动

依赖精确版本见 requirements.txt。在预装依赖的环境中运行 uvicorn brand_signal.api:app --host 0.0.0.0 --port 8000。通过 BRAND_SIGNAL_DB 指定数据库文件，默认是 /tmp/brand-signal.sqlite3。运行 python -m brand_signal.demo 可执行一次性端到端演示。

## 数据流程

先登记市场及 IANA 时区、产品和设备安装记录。可选配置来源字段契约、外部设备号映射和服务响应目标。工单及计量事件使用请求键批量提交；每行分别返回接收或拒绝结果，相同请求键和内容重放原结果。工单变更需要预期修订号。按市场本地日期查询分析指标、跨来源对账、告警及服务复核。

发布候选固定某一源修订及预期发布版本。只有源修订和发布版本未变化时才能发布。旧版本、CSV 和 JSON 导出不可变。迟到数据须进入新候选和新发布版本。市场汇总、比较、抽样与溯源均按明确版本读取。

## 批次撤销与修正重放

当已接收批次来自错误的设备号映射时，使用可追溯的撤销而非删除：

- `POST /batches/{batch_id}/revoke`（带 `reason_code`、`reason`、`idempotency_key`）软撤销批次。原始批次与逐行 payload 永久保留；接受行转为 `revoked` 并记录逐行原因；事件键占用转存到 `event_revocations`（保留原摘要与原批次）后释放，使修正重放不再键冲突；工单置 `revoked` 但 `ticket_history` 全部保留，计量与工单各写一条反向补偿（`record_compensations`）及关联登记。撤销推进全局源修订。
- 撤销后以 `POST /ingest/{source}` 提交修正批次，请求体带 `corrects` 指向被撤销批次。系统只接受该批次原事件键（其它键逐行拒收为 `replay_event_not_in_revoked_batch`），重放沿用同一业务主键取代已撤销记录并写入 `reopen`/重放历史，撤销关联转为 `superseded`，事件键重新占用到有效来源。
- 已撤销记录通过 `status='active'` 的撤销关联从当前工单、计量、对账与质量指标中排除；被修正取代后关联为 `superseded`，记录重新计入且只计一次。
- 工单若在撤销前已有后续状态变迁，`ticket_revocations.had_followup`/`final_state` 记录其终态，全部历史经 `ticket_id` 与补偿记录关联，可由 `GET /tickets/{id}/revocation` 查询；撤销后不允许再变迁（`ticket_revoked`）。
- 未发布候选在源修订推进后自动失效（`ready=false`，`invalid_reasons` 含 `source_revision_superseded`），发布被拒为 `stale_source`；已发布版本及其 CSV/JSON 导出、市场汇总不可变，溯源（`/lineage`）标注来源是否被撤销及撤销是否仍处于 active。
- 撤销以 `idempotency_key` 去重并在 `BEGIN IMMEDIATE` 事务内串行化，重复撤销返回同一结果；重放复用请求键幂等，并发撤销或重复重放均不会双重扣除或重复计数。修正批次本身不可再撤销。

## 验证

python -m unittest discover -s tests -v

python -m compileall -q brand_signal

python -m brand_signal.demo

API 错误返回稳定的 error 代码和 HTTP 4xx 状态。空分母表示为 JSON null。输入时间戳必须包含时区偏移，本地日期格式为 YYYY-MM-DD。
