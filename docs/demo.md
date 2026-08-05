# Demo操作與故障恢復

## Clean start

`make demo`不刪資料。要明確驗證乾淨volumes：

```bash
docker compose down -v
make demo
```

`down -v`是destructive command，會刪除本Compose project的Kafka/PostgreSQL volumes；只應由使用者明確執行。

## Demo流程

```text
start infrastructure → readiness → topics → migration
→ start Order/Log consumers
→ produce normal / duplicate / invalid / error log records
→ wait for exact run offsets to commit
→ query valid_orders / processed_events / log metrics
→ consume matching DLQ records → show final lag
→ gracefully stop consumers
→ produce while stopped → observe lag rise
→ restart consumers → observe lag recovery
→ reproduce DB-commit-before-Kafka-commit window
→ verify replay remains idempotent → cleanup → report
```

Readiness與processing都使用bounded polling，不以固定sleep判定成功。失敗stage回傳非0並仍嘗試寫report。

## Expected evidence

- 本次run有`valid_orders`、`processed_events`與processed access logs。
- duplicate delivered但business row不重複。
- invalid或`application_error_log`存在於DLQ。
- stopped期間真實Kafka lag大於0，restart後本次source offsets全部commit。
- deterministic uncommitted replay前後同event ID只有一筆business row。

## Failure scenario分類

### 真實Consumer process停止

Demo向實際Order/Log child processes送SIGTERM，確認退出後才繼續produce。Recovery起點為replacement啟動前的
monotonic timestamp；終點為本次所有source coordinates低於真實committed next offset。Poll interval 0.25
秒，timeout 60秒。這是真實application process停止，不是container failure。

### Deterministic uncommitted window

Demo以真實Kafka message與PostgreSQL transaction執行Order processing，但刻意省略Kafka commit後關閉
consumer，重現「DB已commit、Kafka offset尚未commit」。正式Order Consumer重啟後讀到同一event，
`processed_events`防止重複寫入`valid_orders`，然後安全commit offset。

這是控制過的crash-window simulation，不是隨機kill，也不是infrastructure failure。

### 尚未覆蓋

- Kafka或PostgreSQL container crash
- network partition
- disk full/corruption
- multi-broker failover

## Cleanup

Runner追蹤每個child process。正常路徑SIGTERM並bounded wait；只有timeout才SIGKILL，forced cleanup使demo
failed。Temporary logs位於temporary directory，沒有PID files。Infrastructure維持running；使用者可執行
`make down`。

## Troubleshooting

- `group_not_established`：確認consumers已啟動並join group。
- `offset_not_available`：該partition尚未有committed offset，不會假造lag 0。
- Existing backlog timeout：先用`make consumer-lag`確認，不要reset固定groups掩蓋問題。
- Topic missing：執行`make topics`。
- PostgreSQL schema missing：執行`make migrate`。
- Kafka unavailable：檢查`docker compose ps`與`docker compose logs kafka`。
- Demo非0：查看該次JSON的`failure_stage`與`errors`。
