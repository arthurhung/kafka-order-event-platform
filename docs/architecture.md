# Phase 1–5 系統架構

Phase 1 建立三個本機基礎服務：

```text
Event Generator／本機工具 ── Kafka protocol ──> Kafka（combined KRaft Broker／Controller）
       │                                      │
       ├── SQL／Alembic ──────────────> PostgreSQL 16
       │
       └── 瀏覽器 ────────────────────> Kafka UI ──> Kafka
```

## 本機 Python 環境

本機 Python 指令使用 pyenv 管理的 `kafka_streaming` virtualenv，Python 版本固定為 3.14.6。
Repository 根目錄的 `.python-version` 會讓 pyenv 在進入專案目錄時自動選擇此環境。

所有共用設定由 `streaming_platform.config` 從環境變數或 `.env` 載入。預設的本機端點為：

- Kafka：`localhost:29092`
- PostgreSQL：`localhost:5432`
- Kafka UI：`localhost:8080`

使用者專屬的 Python 絕對路徑不會寫入應用程式、Docker Compose、Makefile 或共用設定。

## Docker Compose 網路

Compose 內部服務透過預設 Docker network 互相連線：

- Kafka 內部 listener：`kafka:9092`
- Kafka controller listener：`kafka:9093`
- PostgreSQL：`postgres:5432`
- Kafka UI 透過 `kafka:9092` 讀取 cluster 狀態

Kafka 同時提供 `localhost:29092` 給本機 Python 工具使用。內部 listener 與本機 listener 分開，
避免把只在 Docker network 中可解析的 hostname 回傳給本機 client。

## 啟動與健康檢查

Kafka 與 PostgreSQL 都定義了 health check。Kafka UI 只有在 Kafka health check 成功後才啟動，
不只依賴 container 的啟動順序。

`make up` 會執行：

```text
啟動 Kafka、PostgreSQL、Kafka UI
→ 等待 Kafka healthy
→ 啟動 Kafka UI
→ 等待本機 Kafka 與 PostgreSQL TCP endpoint 可連線
```

Topic bootstrap 與 database migration 不會在 container 啟動時隱式執行，必須明確執行：

```bash
make topics
make migrate
```

這讓基礎服務啟動、Kafka metadata 初始化與 database schema 變更維持清楚且可獨立除錯。

## Phase 2 Event Generator

Phase 2 在本機 Python process 中執行 Event Generator：

```text
Pydantic Event Factory
→ normal / schema-invalid / schema-valid stale / duplicate
→ topic + UTF-8 key routing
→ confluent-kafka Producer
→ delivery callback / bounded queue backoff / flush
→ measured JSON report
```

共用 Event Envelope 只拒絕 naive datetime，並將 aware datetime 正規化為 UTC。Order、payment、
access log 與 error log 各自使用符合事件語意的 Payload Model，不以大量 Optional 欄位合併模型。

## Phase 2 邊界

Docker Compose 仍只包含 Kafka、PostgreSQL 與 Kafka UI。Event Generator 是 Phase 2 的本機 Python
application；Order Consumer 與 Log Consumer 尚未建立，也沒有空殼 executable。

資料庫 migration 已建立後續會使用的資料表，但 Phase 2 不包含 event persistence、offset commit、
idempotency、retry、DLQ consumer 或 aggregation 行為。

## Phase 3 Order Consumer

```text
ecommerce.orders.raw.v1
→ UTF-8 / JSON decode
→ Order Pydantic validation
├─ permanent error → delivery-confirmed ecommerce.dlq.v1 → commit source offset
└─ valid event → PostgreSQL transaction
                 ├─ INSERT processed_events ON CONFLICT DO NOTHING
                 └─ new event only: INSERT valid_orders
                 → commit DB
                 → commit Kafka offset
```

Executable composition 位於 `apps/order_consumer`。Kafka polling、DLQ producer、retry、repository 與
processing service 分別位於 `src/streaming_platform`，polling loop 不承擔資料映射或 transaction 邏輯。

Phase 3 不包含 Application Log Consumer、aggregation、window、watermark 或 benchmark。

## Phase 4 Application Log Consumer

```text
ecommerce.application-logs.raw.v1
→ decode / api_access_log validation
├─ permanent error → delivery-confirmed ecommerce.dlq.v1
└─ valid event → event-time UTC minute buffer
                 → snapshot/swap every 10 seconds
                 → one PostgreSQL transaction
                    ├─ processed_events idempotency markers
                    └─ additive log_metrics_minute upsert
                 → per-partition contiguous offset commit
```

Late event 使用自己的 `event_time` 更新舊分鐘。Buffer、aggregation、repository、offset tracker 與
polling orchestration 分層，Kafka loop 不包含 SQL。Phase 4 不加入 window/watermark framework。

## Phase 5 Benchmark、Lag與Demo

```text
Benchmark / Demo Orchestrator
├─ readiness、topic bootstrap、migration
├─ managed Order/Log consumer subprocesses
├─ existing Event Generator subprocess
├─ real Kafka committed offsets + log-end offsets
├─ run-scoped PostgreSQL queries
├─ run-scoped DLQ observation
└─ timestamped UTF-8 JSON report
```

Run identity不修改event或database schema。Producer delivery callback回傳的精確
`topic/partition/offset` intervals是run attribution來源；`processed_events`、`valid_orders`與DLQ都保存可對應
的source coordinates。`log_metrics_minute`是跨run additive table，所以report不把全表row count說成本次run
新增row count。

Phase 5 child processes在temporary directory輸出logs；normal cleanup先SIGTERM，只有bounded timeout後才
SIGKILL。Infrastructure不由benchmark/demo自動刪除。
