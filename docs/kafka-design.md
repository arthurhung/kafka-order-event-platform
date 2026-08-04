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

Order topic 使用 6 個 partitions。後續 Producer 會以 `order_id` 作為 key，讓同一訂單的事件進入
相同 partition，以保留單一訂單內的事件順序；不同訂單仍可分散處理。

Application log topic 同樣使用 6 個 partitions。後續 Producer 會以 `service` 作為 key，讓同一服務的
日誌傾向進入相同 partition，同時保留跨服務的平行處理能力。

DLQ 使用 3 個 partitions，後續 DLQ Producer 會以 `event_id` 作為 key。Phase 1 只建立 topic，尚未
實作 DLQ message schema 與 produce 行為。

Consumer instance 數量不應超過所消費 topic 的 partition 數，否則超出的 Consumer 會處於閒置狀態。

## 單節點限制

所有 topics 的 replication factor 都是 1，符合本機單 Broker 環境，但不提供 Broker failure
redundancy。即使後續 Producer 設定 `acks=all`，在此環境中也只代表唯一副本已確認寫入，不能解讀為
具備多副本耐久性。

Multi-broker Kafka、跨 Broker replication 與 production security 都不在目前 MVP 範圍內。
