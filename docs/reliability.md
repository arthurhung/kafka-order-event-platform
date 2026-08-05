# 可靠性設計說明

## Phase 1–2 已完成的可靠性基礎

Phase 1 讓本機環境的啟動與 schema 初始化具備可重複性：

- Kafka 與 PostgreSQL 都有 health check。
- Kafka UI 會等待 Kafka healthy 後才啟動。
- `make up` 會等待本機 Kafka 與 PostgreSQL endpoint 可連線。
- Kafka 關閉自動建立 topic，topic bootstrap 明確且可重複執行。
- Alembic 管理 PostgreSQL schema，可安全重跑 migration。
- 共用 logging 使用 JSON structured logging。
- 設定由環境變數集中管理。
- `.env` 不提交 Git，log 不應輸出密碼或完整連線字串。
- Event Generator 透過 delivery callback 記錄每筆成功或失敗。
- Producer queue full 使用 1、2、4 秒的 bounded backoff，不會無限重試。
- Generator 正常完成或發生例外時都會在結束前 flush，未確認訊息會計入失敗。
- Generator 收到 Ctrl+C 時會先完成 flush 與 JSON report，再以 130 結束且不輸出 traceback。
- JSON report 的 attempted、delivered、failed 與 injection 數字來自實際執行統計。

## 本機環境限制

目前 Kafka 是 plaintext、單節點、replication factor 1 的開發環境。它沒有 Broker failure
redundancy，因此 `acks=all` 不代表多副本耐久性。PostgreSQL 也使用本機開發用帳號與密碼。

這些設定只用於本機開發、測試與面試展示，不是 production-ready。正式環境至少需要另外評估：

- multi-broker Kafka 與適當的 replication factor；
- TLS、authentication 與 authorization；
- secrets management；
- PostgreSQL 備份、高可用與連線管理；
- monitoring、alerting 與容量規劃。

上述 production 項目不是目前 MVP 的實作範圍。

## Phase 3 Order Consumer

Phase 3 Order Consumer 已實作以下行為：

1. 關閉 Kafka automatic offset commit。
2. 只有在下游處理成功後才能提交 offset。
3. Idempotency record 與 business data 必須寫在同一個 PostgreSQL transaction。
4. PostgreSQL transaction 失敗時不得提交 Kafka offset。
5. 永久性 decoding 或 validation error 必須送入 DLQ。
6. 只有 DLQ message produce 成功後，才能提交原始 message 的 offset。
7. Poison message 不得永久阻塞 partition。
8. Retry 必須有最大次數並採用有上限的 exponential backoff。
9. 重複 `event_id` 不得造成重複 business record。
10. Consumer 以 SIGTERM 與 SIGINT flag 停止 polling，完成目前 critical section 後關閉 Kafka、DLQ
    producer 與 database engine。

Unit tests 驗證分類、retry、DLQ construction、duplicate result 與 commit gate；integration/E2E tests
使用真實 Kafka 與 PostgreSQL 驗證 transaction rollback、offset、DLQ 與 restart replay。

Retry 預設為 initial attempt 加最多 3 次 retry，backoff 為 1、2、4 秒。Retries exhausted 或
non-retryable infrastructure/invariant error 都不提交 offset，Consumer 以非零狀態停止，等待修復後
重啟，不會吞掉錯誤或跳過訊息。

DLQ message 保存原 Topic、Partition、Offset、Key、Payload、Consumer Group、失敗時間與安全錯誤資訊。
Malformed bytes 以 UTF-8 或 base64 保存並標示 encoding。DLQ delivery 失敗時原 offset 保持不變。

## Delivery 語意

本專案的目標是透過 manual offset commit 與 PostgreSQL idempotency，達成可安全重試的
at-least-once processing。Kafka offset 與 PostgreSQL transaction 之間沒有共同的 distributed
transaction，因此不會宣稱跨 Kafka 與 PostgreSQL 的 exactly-once delivery。

若 PostgreSQL 已成功 commit，但 Consumer 在提交 Kafka offset 前停止，message 可能再次被讀取；
後續的 `processed_events` idempotency record 必須讓這類重送不會再次寫入 business data。

## Phase 4 Log Aggregation Reliability

合法 access log 先進入以 `event_id` 去重的記憶體 buffer。Snapshot/swap 讓 flush 期間的新事件留在新
active buffer；DB failure 時舊 snapshot 會合併回去。每次 transaction 先插入 `processed_events` 並
取得真正的新 event IDs，只對這些事件執行 additive minute upsert，因此 DB commit 成功但 Kafka
commit 失敗後的 replay 不會重複累加。

Offset tracker 以 partition 為單位保存 pending/completed 狀態。DB 或 DLQ failure 不會將 offset 標記
完成；只有連續完成的最高位置可同步提交。Retry exhausted、revoke flush failure 或 assignment lost
會使 consumer 明確停止，不會假裝成功。

Graceful SIGINT/SIGTERM 會停止 polling、flush buffer、提交安全 offsets，再關閉 Kafka 與 DB resources。
若 shutdown flush 失敗，offset 保持未提交，process 非零結束，等待 restart replay。

DLQ produce 與 source offset commit 不是共同 transaction，因此 commit failure 後 DLQ record 可能重送。
本設計仍只宣稱 at-least-once delivery 加 idempotent database processing。

## Phase 5 Failure Recovery與量測邊界

`make demo`真實停止consumer processes、在停止期間繼續produce、觀察Kafka lag上升，再啟動replacement
processes。Recovery time使用monotonic clock，從replacement啟動前開始，到本次所有source offsets完成commit；
固定poll interval與timeout都寫入report。

Uncommitted replay使用真實Kafka/PostgreSQL但刻意省略一次Kafka commit，重現DB成功與offset commit間的
window。這是deterministic simulation；不是Kafka、PostgreSQL或network infrastructure failure。

Benchmark以實際committed offsets判斷durable source completion。Observed max lag只代表polling samples中可見
的最大值。Producer delivery callback latency只代表broker acknowledgement；目前沒有把它描述成end-to-end
latency。無法可靠取得的end-to-end與Docker Desktop UI limit使用null/not_implemented/not_available。

每次run採唯一ID與source offset intervals，不reset固定consumer groups、不清使用者資料。Pre-existing backlog
若未在timeout內歸零，run明確失敗而不是把歷史消息混入結果。
