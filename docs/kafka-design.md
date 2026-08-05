# Kafka 設計

## 本機 Cluster

Kafka 使用單一節點，並在同一個 process 中執行 Broker 與 Controller 角色，也就是 combined KRaft
mode。本專案不使用 ZooKeeper，也不在 MVP 階段建立 multi-broker cluster。

Kafka 對外提供兩個資料 listener：

- `INTERNAL://kafka:9092`：供 Docker Compose 內的服務使用
- `EXTERNAL://localhost:29092`：供本機 Python 工具使用

Controller 使用獨立的 `CONTROLLER://kafka:9093` listener。所有 listener 在本機開發環境都使用
plaintext protocol，此設定不是 production-ready。

## Topic 建立方式

Broker 已關閉自動建立 topic。`scripts/create_topics.py` 會透過 Kafka Admin API 明確建立 `SPEC.md`
定義的三個 topics：

| Topic | Partitions | Replication factor | 後續使用的 key | 用途 |
|---|---:|---:|---|---|
| `ecommerce.orders.raw.v1` | 6 | 1 | `order_id` | 訂單與付款事件 |
| `ecommerce.application-logs.raw.v1` | 6 | 1 | `service` | API 與應用程式日誌 |
| `ecommerce.dlq.v1` | 3 | 1 | `event_id` | 永久性處理錯誤 |

Bootstrap script 會先讀取現有 Kafka metadata，只建立不存在的 topics，因此 `make topics` 可以安全地
重複執行。它不會靜默修改已存在 topic 的 partition 數或其他設定。

## Partition 設計

Order topic 使用 6 個 partitions。Phase 2 Event Generator 以 `order_id` 作為 key，讓同一訂單的事件進入
相同 partition，以保留單一訂單內的事件順序；不同訂單仍可分散處理。

Application log topic 同樣使用 6 個 partitions。Phase 2 Event Generator 以 `service` 作為 key，讓同一服務的
日誌傾向進入相同 partition，同時保留跨服務的平行處理能力。

Generator 的 service 候選值為 `order-api`、`payment-api`、`user-api`、`inventory-api`、
`notification-api`、`gateway-api`、`catalog-api`、`auth-api`、`shipping-api`。Producer 不指定
partition，由 librdkafka 預設的 `consistent_random` partitioner 對非空 key 使用 CRC32 hash。
相同 service 會穩定進入同一 partition，不同 service 可能發生 hash collision；候選值數量不應被誤解為
一定會使用相同數量的 partitions。

DLQ 使用 3 個 partitions。Phase 3 DLQ Producer 優先以可驗證的 `event_id` 作為 key；若 malformed
JSON 無法提供 `event_id`，則使用穩定的 `topic:partition:offset` key。DLQ callback 確認 delivery 前，
Order Consumer 不會提交原訊息 offset。

Generator 的 topic 名稱與 bootstrap servers 全部來自集中式環境設定。JSON value 使用 UTF-8，
Producer 透過 delivery callback 統計成功與失敗、定期 `poll()`，並在結束前 `flush()`。單節點環境
仍不可把 delivery acknowledgement 解讀為多副本 durability。

Consumer instance 數量不應超過所消費 topic 的 partition 數，否則超出的 Consumer 會處於閒置狀態。

## Phase 3 Order Consumer Group

Order Consumer 透過設定加入 `order-processing-group-v1`，並同時關閉 `enable.auto.commit` 與
`enable.auto.offset.store`。每筆訊息完成 PostgreSQL transaction 或 DLQ delivery 後，才以 synchronous
commit 提交下一個 offset。

## Phase 4 Application Log Consumer Group

Log Consumer 加入 `application-log-processing-group-v1`，同樣關閉 automatic commit 與 automatic
offset storage。合法事件只在記憶體 buffer 中時保持 pending；DB transaction 成功後才標記 completed。
DLQ 事件則在 broker delivery acknowledgement 後標記 completed。

每個 partition 分別追蹤 pending/completed offsets，只提交從前次 committed position 開始連續完成的
最高 offset + 1。較後面的 invalid event 即使先完成 DLQ，也不能越過較早、尚未 flush 的合法事件。
Rebalance revoke 會先安全 flush，再提交被 revoke partitions 的 contiguous offsets；assignment lost
時不嘗試宣稱 commit 成功。

## 單節點限制

所有 topics 的 replication factor 都是 1，符合本機單 Broker 環境，但不提供 Broker failure
redundancy。即使後續 Producer 設定 `acks=all`，在此環境中也只代表唯一副本已確認寫入，不能解讀為
具備多副本耐久性。

Multi-broker Kafka、跨 Broker replication 與 production security 都不在目前 MVP 範圍內。
