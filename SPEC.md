# Kafka Order Event Platform — MVP Specification

## Project Status

Phase 1–5 define the completed Kafka event-processing MVP baseline.

Post-MVP data platform work, including dbt, data contracts,
CI/CD, MCP tools and AI agents, is defined in:

- `DATA_PLATFORM_SPEC.md`

Changes made under the data-platform extension must not silently
alter the reliability semantics defined in this document.

## 0. 文件定位

本文件是 `kafka-order-event-platform` 的唯一功能規格來源。專案採分階段實作，每個 Phase 都有明確範圍、驗收條件與停止點。

Codex 必須遵守：

1. 一次只執行一個 Phase。
2. 前一階段未驗收完成，不得開始下一階段。
3. 實作前先列出需求摘要、預計修改檔案、風險與測試計畫。
4. 完成後必須實際執行驗證命令，不可只宣稱成功。
5. 不得自行加入 MVP 範圍外技術。

---

# 1. 專案概述

## 1.1 專案名稱

```text
kafka-order-event-platform
```

## 1.2 一句話說明

> 使用 Kafka 建立即時訂單事件與應用程式日誌處理平台，展示 Topic、Partition、Consumer Group、Manual Offset Commit、冪等處理、Retry、DLQ、PostgreSQL 落地、Consumer Lag 與壓力測試。

## 1.3 作品集目標

本作品應能證明：

- Kafka Producer / Consumer 實作能力
- Topic 與 Partition 設計能力
- Kafka Key 與單一 Partition 內順序概念
- Consumer Group 與水平擴充概念
- at-least-once 處理語意
- Manual Offset Commit
- Idempotent Consumer
- Retry 與 bounded backoff
- Poison Message 與 DLQ
- PostgreSQL transaction boundary
- 即時應用日誌聚合
- Consumer Lag 與壓力測試
- 本機開發環境與正式環境差異

## 1.4 不得宣稱

- Production-ready
- 跨 Kafka 與 PostgreSQL 的 exactly-once
- 單節點具備高可用
- 本機測試數據可代表正式環境容量

---

# 2. 開發環境

## 2.1 Python

固定使用：

```text
Python 3.14.6
```

由 pyenv 管理。專案使用名為 `kafka_streaming` 的 pyenv virtualenv；該 virtualenv 必須由
Python 3.14.6 建立。Repository 根目錄必須包含：

```text
.python-version
```

內容：

```text
kafka_streaming
```

`pyproject.toml`：

```toml
[project]
requires-python = ">=3.14,<3.15"
```

驗證命令：

```bash
cat .python-version
python --version
pyenv version
python -c "import sys; print(sys.executable)"
```

不得在程式、Makefile、Dockerfile 或共享設定中寫死：

```text
/Users/arthur/.pyenv/versions/kafka_streaming/bin/python
```

## 2.2 技術棧

- Python 3.14.6
- Apache Kafka，KRaft mode
- PostgreSQL 16
- Kafka UI
- Docker / Docker Compose
- confluent-kafka
- Pydantic 2.x
- pydantic-settings
- SQLAlchemy 2.x
- Alembic
- pytest / pytest-cov
- Ruff
- mypy

## 2.3 Kafka 本機部署

MVP 使用：

```text
1 個 Kafka process
Broker + KRaft Controller combined mode
Replication Factor = 1
```

此模式只用於本機開發與功能驗證，不能展示 Broker Failover、多副本恢復、Controller quorum 容錯與正式環境耐久性。

---

# 3. MVP 範圍

## 3.1 包含

- Kafka 單節點 KRaft
- PostgreSQL
- Kafka UI
- Topic Bootstrap
- Alembic Migration
- Event Models
- 可調整 EPS 的 Event Generator
- 訂單事件與應用程式日誌事件
- Order Consumer
- Log Consumer
- Pydantic Validation
- Manual Offset Commit
- Idempotent Consumer
- Retry
- DLQ
- PostgreSQL Persistence
- 每分鐘日誌聚合
- Consumer Lag
- Benchmark Report
- Unit / Integration / E2E Test
- Makefile
- README 與技術文件

## 3.2 MVP 不包含

- Flink
- Spark
- Debezium
- Kafka Connect
- Schema Registry
- ClickHouse / Doris / Hologres
- Prometheus / Grafana
- Kubernetes
- Terraform
- Cloud Deployment
- Multi-Broker Kafka
- Multi-Controller KRaft

---

# 4. 系統架構

```text
                         ┌──────────────────────┐
                         │   Event Generator    │
                         │ EPS / duration / mix │
                         └──────────┬───────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
              Order Event Producer         Log Event Producer
                     │                             │
                     ▼                             ▼
       ecommerce.orders.raw.v1     ecommerce.application-logs.raw.v1
                     │                             │
                     ▼                             ▼
              Order Consumer                Log Consumer
         validate / retry / dedupe      validate / aggregate
                     │                             │
             ┌───────┴────────┐                    │
             │                │                    │
             ▼                ▼                    ▼
        PostgreSQL      ecommerce.dlq.v1      PostgreSQL
        valid_orders    invalid messages      log_metrics_minute
        processed_events
```

---

# 5. 目標 Repository 結構

```text
kafka-order-event-platform/
├── AGENTS.md
├── SPEC.md
├── README.md
├── Makefile
├── docker-compose.yml
├── pyproject.toml
├── .python-version
├── .env.example
├── .gitignore
│
├── apps/
│   ├── event_generator/
│   ├── order_consumer/
│   └── log_consumer/
│
├── src/
│   └── kafka_order_event_platform/
│       ├── config.py
│       ├── logging.py
│       ├── kafka/
│       ├── models/
│       ├── database/
│       └── metrics/
│
├── migrations/
├── scripts/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── reports/
└── docs/
```

可依實際實作調整檔案切分，但不可靜默變更 Topic、Schema、Consumer Group、交易邊界與 Phase 範圍。

---

# 6. 環境變數

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_ORDER_TOPIC=ecommerce.orders.raw.v1
KAFKA_LOG_TOPIC=ecommerce.application-logs.raw.v1
KAFKA_DLQ_TOPIC=ecommerce.dlq.v1
KAFKA_ORDER_CONSUMER_GROUP=order-processing-group-v1
KAFKA_LOG_CONSUMER_GROUP=application-log-processing-group-v1

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=streaming
POSTGRES_USER=streaming
POSTGRES_PASSWORD=streaming

LOG_LEVEL=INFO
```

規則：

- 提交 `.env.example`
- 不提交 `.env`
- 不 hard-code 密碼、Host、Port、Topic 與 Consumer Group
- 缺少必要設定時要清楚失敗
- Log 不可輸出密碼或完整 connection string

---

# 7. Kafka Topic 規格

| Topic | Partitions | Replication Factor | Key | 用途 |
|---|---:|---:|---|---|
| `ecommerce.orders.raw.v1` | 6 | 1 | `order_id` | 訂單與付款事件 |
| `ecommerce.application-logs.raw.v1` | 6 | 1 | `service` | API 與應用程式日誌 |
| `ecommerce.dlq.v1` | 3 | 1 | `event_id` | 永久性錯誤事件 |

規則：

- 不依賴 auto topic creation
- Topic Bootstrap 必須可重複執行
- Topic 名稱包含版本
- 相同 `order_id` 進入相同 Partition
- 同一 Partition 內才有順序保證
- Consumer 數量大於 Partition 數量不會增加有效平行度

Kafka CLI 的 image-specific 路徑必須封裝於 Makefile 或 scripts，使用者不應需要知道 `/opt/kafka/bin` 或 `/opt/bitnami/kafka/bin`。

必須提供：

```bash
make topics
make list-topics
make describe-topics
make smoke-kafka
```

---

# 8. 共用 Event Envelope

```json
{
  "event_id": "f2a89c11-5ef2-41df-a337-3c44902b2340",
  "event_type": "order_created",
  "event_version": 1,
  "event_time": "2026-08-03T10:00:00.000Z",
  "source": "order-api",
  "payload": {}
}
```

必要欄位：

| 欄位 | 類型 | 說明 |
|---|---|---|
| `event_id` | UUID | 全域唯一事件 ID |
| `event_type` | string | 事件類型 |
| `event_version` | integer | Schema 版本 |
| `event_time` | UTC datetime | 事件發生時間 |
| `source` | string | 事件來源 |
| `payload` | object | 業務內容 |

Envelope 規則：

- 新事件必須產生唯一 `event_id`
- Duplicate 測試事件故意重用既有 `event_id`

## 8.1 Event Time Rules

- `event_time` 必須是 timezone-aware datetime
- 不接受 naive datetime
- `event_time` 在模型驗證後必須正規化為 UTC，並以 UTC 序列化與儲存
- Base Event Model 不限制事件距離目前時間多久
- 舊事件可能代表 late event、replay 或 backfill，不得只因時間較舊而被共用模型拒絕

Event Generator 可以產生 stale event，但 stale event 必須保持 schema-valid，且必須與
schema-invalid event 分開注入、驗證與統計。

---

# 9. Order Event 規格

事件類型：

- `order_created`
- `order_paid`
- `order_cancelled`
- `payment_failed`

範例：

```json
{
  "event_id": "f2a89c11-5ef2-41df-a337-3c44902b2340",
  "event_type": "order_created",
  "event_version": 1,
  "event_time": "2026-08-03T10:00:00.000Z",
  "source": "order-api",
  "payload": {
    "order_id": "ORD-100001",
    "user_id": "USR-1001",
    "product_id": "PRD-501",
    "quantity": 2,
    "amount": "1800.00",
    "currency": "TWD",
    "channel": "web"
  }
}
```

每個 Order Event Type 必須使用獨立 Payload Model，不得以一個包含大量 Optional 欄位的
共用 Order Payload 取代：

## 9.1 OrderCreatedPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `order_id` | 是 | 非空字串 |
| `user_id` | 是 | 非空字串 |
| `product_id` | 是 | 非空字串 |
| `quantity` | 是 | `> 0` |
| `amount` | 是 | Decimal，`> 0` |
| `currency` | 是 | `TWD`、`USD` |
| `channel` | 是 | `web`、`ios`、`android` |

## 9.2 OrderPaidPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `order_id` | 是 | 非空字串 |
| `user_id` | 是 | 非空字串 |
| `payment_id` | 是 | 非空字串 |
| `amount` | 是 | Decimal，`> 0` |
| `currency` | 是 | `TWD`、`USD` |
| `payment_method` | 是 | 非空字串 |

## 9.3 OrderCancelledPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `order_id` | 是 | 非空字串 |
| `user_id` | 是 | 非空字串 |
| `cancellation_reason` | 是 | 非空字串 |

## 9.4 PaymentFailedPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `order_id` | 是 | 非空字串 |
| `user_id` | 是 | 非空字串 |
| `payment_id` | 是 | 非空字串 |
| `amount` | 是 | Decimal，`> 0` |
| `currency` | 是 | `TWD`、`USD` |
| `failure_code` | 是 | 非空字串 |
| `failure_reason` | 是 | 非空字串 |

Event Model 與 Payload Model 的對應必須固定：

| `event_type` | Payload Model |
|---|---|
| `order_created` | `OrderCreatedPayload` |
| `order_paid` | `OrderPaidPayload` |
| `order_cancelled` | `OrderCancelledPayload` |
| `payment_failed` | `PaymentFailedPayload` |

所有金額欄位使用 Decimal，不使用 Python float 進行金額資料庫寫入。

---

# 10. Application Log Event 規格

事件類型：

- `api_access_log`
- `application_error_log`

範例：

```json
{
  "event_id": "c4bb2622-a2fc-42fc-b27f-366f48b521b5",
  "event_type": "api_access_log",
  "event_version": 1,
  "event_time": "2026-08-03T10:00:01.123Z",
  "source": "order-api",
  "payload": {
    "request_id": "REQ-100001",
    "service": "order-api",
    "endpoint": "/orders",
    "http_method": "POST",
    "status_code": 201,
    "response_time_ms": 135,
    "client_ip": "10.0.0.15"
  }
}
```

每個 Log Event Type 必須使用獨立 Payload Model，不得以一個包含大量 Optional 欄位的
共用 Log Payload 取代。

HTTP Method 建議以 string Enum 實作，允許值固定為：

- `GET`
- `POST`
- `PUT`
- `PATCH`
- `DELETE`
- `HEAD`
- `OPTIONS`

其他 HTTP Method 必須產生 validation error。

## 10.1 ApiAccessLogPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `request_id` | 是 | 非空字串 |
| `service` | 是 | 非空字串 |
| `endpoint` | 是 | 以 `/` 開頭 |
| `http_method` | 是 | HTTP Method allowlist |
| `status_code` | 是 | 100 到 599 |
| `response_time_ms` | 是 | `>= 0` |
| `client_ip` | 是 | 非空字串 |

## 10.2 ApplicationErrorLogPayload

| 欄位 | 必填 | 驗證規則 |
|---|---|---|
| `request_id` | 是 | 非空字串 |
| `service` | 是 | 非空字串 |
| `error_type` | 是 | 非空字串 |
| `error_message` | 是 | 非空字串 |
| `endpoint` | 否 | 若提供，必須以 `/` 開頭 |
| `stack_trace` | 否 | 字串 |
| `trace_id` | 否 | 非空字串 |

Event Model 與 Payload Model 的對應必須固定：

| `event_type` | Payload Model |
|---|---|
| `api_access_log` | `ApiAccessLogPayload` |
| `application_error_log` | `ApplicationErrorLogPayload` |

---

# 11. PostgreSQL Schema

## 11.1 processed_events

```sql
CREATE TABLE processed_events (
    consumer_group VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
);
```

## 11.2 valid_orders

```sql
CREATE TABLE valid_orders (
    event_id UUID PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64),
    quantity INTEGER,
    amount NUMERIC(18, 2),
    currency VARCHAR(3),
    channel VARCHAR(20),
    event_time TIMESTAMPTZ NOT NULL,
    kafka_topic VARCHAR(255) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

必要索引：

```sql
CREATE INDEX idx_valid_orders_order_id ON valid_orders(order_id);
CREATE INDEX idx_valid_orders_event_time ON valid_orders(event_time);
```

## 11.3 log_metrics_minute

```sql
CREATE TABLE log_metrics_minute (
    metric_minute TIMESTAMPTZ NOT NULL,
    service VARCHAR(100) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    request_count BIGINT NOT NULL,
    success_count BIGINT NOT NULL,
    client_error_count BIGINT NOT NULL,
    server_error_count BIGINT NOT NULL,
    response_time_sum_ms BIGINT NOT NULL,
    max_response_time_ms INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_minute, service, endpoint)
);
```

交易規則：

- SQLAlchemy 2.x style
- Alembic 管理 Migration
- `processed_events` 與業務資料寫入使用同一 DB Transaction
- DB 失敗不可 Commit Kafka Offset

---

# 12. DLQ 規格

```json
{
  "failed_at": "2026-08-03T10:10:00.000Z",
  "error_type": "ValidationError",
  "error_message": "amount must be greater than 0",
  "original_topic": "ecommerce.orders.raw.v1",
  "original_partition": 2,
  "original_offset": 1035,
  "consumer_group": "order-processing-group-v1",
  "original_key": "ORD-100001",
  "original_payload": {}
}
```

規則：

- 保留原始 Topic、Partition、Offset、Key、Payload
- 包含錯誤類型與安全的錯誤訊息
- JSON decode、Validation、unsupported event type 屬永久性錯誤
- DLQ Produce 成功後才 Commit 原始 Offset
- Poison Message 不可永久卡住 Partition

---

# 13. Retry 規格

Retryable：

- PostgreSQL connection timeout
- PostgreSQL 暫時不可用
- Kafka temporary transport error

Non-retryable：

- Invalid JSON
- Pydantic Validation Error
- Unsupported Event Type
- 不合理的業務值

策略：

```text
最多 3 次
Backoff：1 秒、2 秒、4 秒
```

不得無限 Retry，不得吞掉例外。

---

# 14. Structured Logging

必要欄位：

```json
{
  "timestamp": "2026-08-03T10:00:00.000Z",
  "level": "INFO",
  "service": "order-consumer",
  "message": "event processed",
  "event_id": "uuid",
  "topic": "ecommerce.orders.raw.v1",
  "partition": 1,
  "offset": 100,
  "processing_time_ms": 12
}
```

不得記錄：密碼、Token、完整 Connection String、無必要個資與敏感 Payload。

---

# 15. Makefile 介面

最終 MVP 應提供：

```bash
make help
make up
make down
make restart
make logs
make topics
make list-topics
make describe-topics
make smoke-kafka
make migrate
make generate-smoke
make generate-standard
make generate-stress
make inject-bad-events
make consumer-lag
make lint
make format
make typecheck
make test
make test-unit
make test-integration
make test-e2e
make benchmark
make demo
make clean
```

Target 必須實際執行工作，不能只 `echo passed`。

---

# 16. 分階段實作

## Phase 1 — Project Skeleton and Infrastructure

### 目標

建立可重複啟動、初始化、通訊、重啟並保存資料的本機環境。

### 範圍

- Python 3.14.6 設定
- `.python-version`
- `pyproject.toml`
- Package Skeleton
- Centralized Settings
- Structured Logging Foundation
- Docker Compose
- Kafka KRaft 單節點
- PostgreSQL
- Kafka UI
- Topic Bootstrap
- Alembic Setup
- Makefile
- README 啟動說明
- 基礎測試

### 功能驗收

- [ ] `.python-version` 為 `kafka_streaming`
- [ ] `pyenv version` 顯示 `kafka_streaming`
- [ ] `python --version` 為 3.14.6
- [ ] `docker compose config` 成功
- [ ] Kafka、PostgreSQL、Kafka UI 正常啟動
- [ ] 三個 Topic 正確建立
- [ ] `make topics` 可連續執行兩次
- [ ] Partition 數量符合規格
- [ ] `make list-topics` 成功
- [ ] `make describe-topics` 成功
- [ ] `make smoke-kafka` 可實際 Produce / Consume
- [ ] Kafka Key 可正確寫入與讀取
- [ ] Migration 可連續執行兩次
- [ ] PostgreSQL 可實際 Insert / Select
- [ ] Restart 後 Topic、Kafka Message、DB Schema 與資料仍存在
- [ ] `make lint` 通過
- [ ] `make typecheck` 通過
- [ ] `make test` 通過

### Completion Gate

```bash
make up
make topics
make list-topics
make describe-topics
make smoke-kafka
make migrate
make lint
make typecheck
make test
```

### Phase 1 不做

Event Generator、Order Consumer、Log Consumer、DLQ 邏輯、冪等處理、Benchmark、Flink、CDC。

---

## Phase 2 — Event Models and Event Generator

### 目標

依指定流量產生訂單與應用程式日誌事件，使用正確 Topic 與 Kafka Key 發送。

### 範圍

- Common Event Envelope
- Order Event Models
- Log Event Models
- Event Factory
- Deterministic Random Seed
- Kafka Producer Wrapper
- Configurable EPS
- Duration
- Event Mix
- Invalid Injection
- Stale Event Injection
- Duplicate Injection
- Delivery Callback
- Producer Report

### CLI

```bash
python -m apps.event_generator \
  --events-per-second 1000 \
  --duration-seconds 60 \
  --order-ratio 0.2 \
  --log-ratio 0.8 \
  --invalid-rate 0.02 \
  --stale-rate 0.01 \
  --stale-hours 168 \
  --duplicate-rate 0.05 \
  --seed 42 \
  --report-path reports/latest.json
```

CLI 參數規則：

- `--invalid-rate`、`--stale-rate`、`--duplicate-rate` 的範圍皆為 0 到 1
- normal、invalid、stale、duplicate 是互斥的事件分類
- `invalid-rate + stale-rate + duplicate-rate` 不得大於 1
- `--stale-hours` 必須大於 0，表示 stale event 的 `event_time` 比產生當下早多少小時
- stale event 不得計入 `--invalid-rate`

### Producer 行為

- 使用 confluent-kafka
- JSON UTF-8 序列化
- Order Key = order_id
- Log Key = service
- 定期 `poll()`
- Queue Full 時 bounded backoff
- Delivery Callback
- 結束前 `flush()`
- 每 5 秒輸出進度
- 記錄 attempted、delivered、failed、actual EPS
- 分別記錄 schema-invalid、stale、duplicate injection 數量

### Invalid Injection

Schema-invalid event 包含：

- 缺少必要欄位
- 負數 amount
- Invalid currency
- Invalid channel
- Invalid HTTP method
- Invalid status code
- Unsupported event type
- Malformed payload

規則：

- Invalid injection 必須從合法事件建立可路由的原始 Topic 與 Kafka Key，再破壞 schema
- 僅對具有對應欄位的 Event Type 套用該 invalid 變體
- schema-invalid event 必須計入 `invalid_events_injected`
- stale event 不屬於 schema-invalid event

### Stale Event Injection

- stale event 的 `event_time` 為產生當下 UTC 時間減去 `--stale-hours`
- stale event 除 `event_time` 較舊外，Envelope、Event Type 與 Payload 都必須 schema-valid
- 共用 Pydantic Event Model 必須接受 stale event
- stale event 可代表 late event、replay 或 backfill，不得因時間較舊而自動視為錯誤
- stale event 必須計入 `stale_events_injected`，不可計入 `invalid_events_injected`

### Duplicate Injection

Duplicate 必須從先前已產生的事件重送，至少重用相同 `event_id`，不得為 duplicate 產生新的
`event_id`。Duplicate 必須計入 `duplicate_events_injected`，並與 invalid、stale 分開統計。

### Producer Report

Producer 結束後必須在 `--report-path` 寫入 UTF-8 JSON report。以下只是 report schema 範例，
不代表 benchmark 實測結果：

```json
{
  "report_version": 1,
  "started_at": "2026-08-03T10:00:00.000Z",
  "finished_at": "2026-08-03T10:01:00.250Z",
  "target_events_per_second": 1000,
  "duration_seconds": 60,
  "actual_events_per_second": 997.4,
  "attempted": 60000,
  "delivered": 59844,
  "failed": 156,
  "order_events_attempted": 12000,
  "log_events_attempted": 48000,
  "invalid_events_injected": 1200,
  "stale_events_injected": 600,
  "duplicate_events_injected": 3000,
  "invalid_events_by_type": {
    "missing_required_field": 150,
    "negative_amount": 150,
    "invalid_currency": 150,
    "invalid_channel": 150,
    "invalid_http_method": 150,
    "invalid_status_code": 150,
    "unsupported_event_type": 150,
    "malformed_payload": 150
  },
  "seed": 42,
  "stale_hours": 168
}
```

Report 中所有執行結果必須來自實際 delivery callback、flush 與 generator 統計，不得填入虛構
效能數據。至少必須分別包含：

- `invalid_events_injected`
- `stale_events_injected`
- `duplicate_events_injected`

### Phase 2 Unit Test 規格

- Base Event Model 接受 timezone-aware datetime、拒絕 naive datetime
- 非 UTC timezone-aware datetime 會正規化並序列化為 UTC
- Base Event Model 接受 schema-valid stale event，不限制事件距離現在多久
- 六種 Event Type 使用正確且獨立的 Payload Model
- 每種 Payload 的 required field 與欄位驗證正確
- Order 金額使用 Decimal，正數限制正確
- HTTP Method string Enum 接受 allowlist 並拒絕其他 method
- Event Factory 在相同 seed 下產生可重現的事件選擇與資料
- 新事件使用唯一 `event_id`
- Order / Log ratio 與三種 injection rate 驗證正確
- 八種 schema-invalid injection 可產生預期的 validation failure
- stale injection 保持 schema-valid，時間依 `stale-hours` 往前移動並獨立統計
- duplicate injection 重用既有 `event_id` 並獨立統計
- Topic routing 與 Kafka message key 計算正確
- Delivery callback、Queue Full bounded backoff、flush 與 report calculation 正確

### Phase 2 Integration Test 規格

Phase 2 Integration Test 使用真實 Kafka container，但不需要 PostgreSQL，也不得實作正式
Consumer application。測試可以使用有 timeout 的臨時 test consumer 讀回 Producer 訊息：

- Order Event 寫入 Order Topic，Kafka Key 等於 `order_id`
- Application Log Event 寫入 Log Topic，Kafka Key 等於 `service`
- 六種 Event Type 均可經由 Producer 送達並以對應 Pydantic Model 驗證
- schema-invalid event 可送到所屬 raw topic，讀回後會產生預期 validation error
- stale event 可送達、讀回後仍通過 schema validation，且 report 僅增加 stale 統計
- duplicate event 至少有兩筆訊息使用相同 `event_id`，且 report 獨立統計 duplicate
- Delivery callback 的成功與失敗統計會反映於 report
- 所有 consume/poll 等待必須有 timeout，不可永久等待
- 不驗證 Manual Offset、DB、Idempotency、DLQ 或 Phase 3 Consumer 行為

### Phase 2 驗收

- [ ] CLI 可使用
- [ ] Topic Routing 正確
- [ ] Kafka Key 正確
- [ ] EPS / Duration / Mix 可調
- [ ] Base Event Model 拒絕 naive datetime，並將 aware datetime 正規化及序列化為 UTC
- [ ] Base Event Model 接受 schema-valid stale event，不以事件年齡作全域拒絕
- [ ] 四種 Order Event 與兩種 Log Event 使用各自的 Payload Model
- [ ] HTTP Method allowlist 驗證正確
- [ ] Invalid Injection 可運作
- [ ] Stale Injection 可運作且保持 schema-valid
- [ ] Duplicate Injection 可運作
- [ ] Invalid、Stale、Duplicate Injection 分開統計
- [ ] Delivery Failure 可被統計
- [ ] Producer 結束前 Flush
- [ ] JSON Report 包含 `invalid_events_injected`、`stale_events_injected`、`duplicate_events_injected`
- [ ] Unit Test 通過
- [ ] Integration Test 通過
- [ ] lint / typecheck 通過

### Completion Gate

```bash
make generate-smoke
make test-unit
make test-integration
make lint
make typecheck
```

### Phase 2 不做

Order Consumer、Manual Offset、DB Idempotency、DLQ Consumer、Log Aggregation、Benchmark。

---

## Phase 3 — Order Consumer, Idempotency, Retry and DLQ

### 目標

可靠處理訂單事件，實作 Manual Offset、DB Transaction、Idempotency、Retry 與 DLQ。

### Consumer Group

```text
order-processing-group-v1
```

### 正常流程

```text
Poll
→ JSON Decode
→ Pydantic Validate
→ Begin DB Transaction
→ Insert processed_events
→ Insert valid_orders
→ Commit DB
→ Commit Kafka Offset
```

### Duplicate 流程

```text
event_id 已存在
→ 不重複寫入業務資料
→ 安全 Commit Offset
```

### 永久錯誤流程

```text
Decode / Validation Error
→ Produce DLQ
→ Confirm DLQ Delivery
→ Commit Original Offset
```

### 暫時性 DB 錯誤流程

```text
Rollback
→ bounded retry
→ success：Commit DB + Offset
→ exhausted：明確停止或 failed state
```

### Phase 3 驗收

- [ ] Auto Commit 關閉
- [ ] DB 成功後才 Commit Offset
- [ ] 合法事件寫入 valid_orders
- [ ] Duplicate 不重複寫入
- [ ] `processed_events` 與業務資料同 Transaction
- [ ] 永久錯誤進 DLQ
- [ ] DLQ 成功後才 Commit 原 Offset
- [ ] DB Failure 不 Commit Offset
- [ ] Retry 有上限
- [ ] Consumer 支援 SIGTERM / SIGINT
- [ ] Consumer Restart 可安全重處理未 Commit Event
- [ ] Poison Message 不永久阻塞
- [ ] Unit / Integration / E2E Test 通過
- [ ] lint / typecheck 通過

### Completion Gate

```bash
make test-unit
make test-integration
make test-e2e
make lint
make typecheck
```

### Phase 3 不做

Flink、ClickHouse、Watermark、Window、Log Aggregation、完整 Benchmark。

---

## Phase 4 — Application Log Consumer and Minute Aggregation

### 目標

消費大量應用程式日誌並產生每分鐘可重複使用的服務指標。

### Consumer Group

```text
application-log-processing-group-v1
```

### Aggregation Key

```text
(metric_minute, service, endpoint)
```

### 指標

- request_count
- success_count
- client_error_count
- server_error_count
- response_time_sum_ms
- average_response_time_ms
- max_response_time_ms

分類：

```text
100–399 success
400–499 client error
500–599 server error
```

### MVP 聚合策略

1. 驗證 Event
2. `event_time` 截斷到 UTC 分鐘
3. 更新記憶體 Buffer
4. 每 10 秒 Flush
5. PostgreSQL Upsert
6. Graceful Shutdown 前 Flush

Late Event 可更新舊分鐘資料，但此階段不宣稱 Flink Watermark。

### Phase 4 驗收

- [ ] Log Consumer 加入正確 Consumer Group
- [ ] Event Validation 正常
- [ ] 每分鐘聚合正確
- [ ] PostgreSQL Upsert 正確
- [ ] 4xx / 5xx 統計正確
- [ ] Late Event 可更新舊分鐘
- [ ] Invalid Event 進 DLQ
- [ ] Shutdown 會 Flush Buffer
- [ ] Offset Strategy 有文件與測試
- [ ] Unit / Integration / E2E Test 通過
- [ ] lint / typecheck 通過

### Completion Gate

```bash
make test-unit
make test-integration
make test-e2e
make lint
make typecheck
```

---

## Phase 5 — Benchmark, Consumer Lag, Demo and Documentation

### 目標

提供可重複的效能證據、Consumer Lag、故障恢復 Demo 與完整 GitHub 文件。

### Workloads

```text
Smoke:    100 EPS / 60 seconds
Standard: 1,000 EPS / 300 seconds
Stress:   5,000 EPS / 300 seconds
```

Stress 可依本機能力調整，但必須記錄硬體與 Docker 資源。

### 必記錄指標

- target EPS
- actual produced EPS
- attempted / delivered / failed
- consumed count
- duplicate count
- DLQ count
- average latency
- P95 / P99 latency
- max / final Consumer Lag
- runtime
- Consumer Restart Recovery Time

不得虛構數據；未完成的指標應標示未實作。

### Consumer Lag

```bash
make consumer-lag
```

至少顯示：

- Consumer Group
- Topic
- Partition
- Current Offset
- Log End Offset
- Lag

### Demo 必須展示

1. 正常混合事件
2. Order 寫入 PostgreSQL
3. Log Minute Aggregation
4. Duplicate 不重複寫入
5. Invalid Event 進 DLQ
6. 停止 Consumer 後 Lag 上升
7. 重啟 Consumer 後 Lag 下降
8. 未 Commit Event 安全重處理
9. Restart 後資料仍存在

### `make demo`

```text
Start Infrastructure
→ Wait Ready
→ Create Topics
→ Migrate
→ Start Consumers
→ Produce Normal / Duplicate / Invalid Events
→ Query PostgreSQL
→ Show DLQ
→ Show Consumer Lag
→ Generate Report
```

### README 必須包含

- 專案簡介
- 問題與架構圖
- 技術棧
- Topic / Partition / Key 設計
- Consumer Group
- 啟動方式
- Demo
- Event Schema
- Manual Offset
- Idempotency
- Retry / DLQ
- PostgreSQL Model
- Benchmark 方法與實測結果
- 本機限制
- Roadmap
- 面試重點

### Phase 5 驗收

- [ ] Benchmark 可執行
- [ ] Report JSON 產出
- [ ] Consumer Lag 可查
- [ ] `make demo` 可由乾淨環境執行
- [ ] Failure Scenarios 可重現
- [ ] README 與實作一致
- [ ] 無虛構數據
- [ ] 完整 Test Suite 通過
- [ ] lint / typecheck 通過

### Completion Gate

```bash
make demo
make benchmark
make consumer-lag
make lint
make typecheck
make test
```

---

# 17. 測試策略

## Unit Test

- Phase 2：Base Event Model timezone-aware / UTC normalization / stale acceptance
- Phase 2：六種獨立 Payload Model 與 HTTP Method string Enum
- Pydantic Validation
- Decimal
- UTC Datetime
- Event Factory
- Ratio Validation
- Invalid / Stale / Duplicate Injection 分類與統計
- Duplicate Reuse
- Aggregation
- Retry Backoff
- DLQ Message
- Report Calculation

## Integration Test

重要整合行為不得全部 Mock。Phase 2 使用真實 Kafka container；進入需要資料庫的 Phase 3、
Phase 4 與 E2E 時，才同時使用真實 PostgreSQL container。

Phase 2 驗證：

- Python Producer Delivery
- Topic Routing
- Key Routing
- Consumer Read
- 六種 Event Type round trip
- Schema-invalid event validation failure
- Schema-valid stale event round trip
- Duplicate `event_id` reuse
- Producer report injection 與 delivery 統計

後續 Phase 驗證：

- Valid Persistence
- Idempotency
- DLQ
- Database Failure
- Offset Behavior
- Aggregation Upsert

不得把所有 Kafka 與 DB Interaction 都 Mock 掉。

## E2E Test

```text
Start Infrastructure
→ Create Topics
→ Migrate
→ Start Consumers
→ Send Mixed Events
→ Verify valid_orders
→ Verify log_metrics_minute
→ Verify DLQ
→ Verify Duplicates
→ Verify Lag Decreases
```

測試必須有 timeout，不可永久等待訊息。

---

# 18. Coding Standards

- Python Type Hints
- 公開函式與類別有 Docstring
- 商業邏輯不放在 CLI 或 Kafka Poll Loop
- 避免過長函式
- 不使用 bare except
- 不吞掉 exception
- 保留 exception context
- UTC timezone-aware datetime
- Decimal 金額
- Structured Logging
- Graceful Shutdown
- Producer 與 Buffer 在結束前 Flush
- 程式要能在面試時清楚解釋

---

# 19. 整體 Definition of Done

- [ ] Phase 1–5 全部完成
- [ ] Clean Checkout 可啟動
- [ ] Topic / Migration 可重複初始化
- [ ] Event Generator 可用
- [ ] Order Consumer 可靠且冪等
- [ ] Log Consumer 聚合正確
- [ ] Invalid Event 進 DLQ
- [ ] Manual Offset 行為有測試
- [ ] Consumer Lag 可見
- [ ] Benchmark 可重現
- [ ] Failure / Restart 可展示
- [ ] README 正確
- [ ] Unit / Integration / E2E 全通過
- [ ] lint / typecheck 通過
- [ ] 不宣稱 Production-ready 或 Exactly-once
- [ ] 不虛構效能數據

---

# 20. Post-MVP Roadmap

## CDC

- Debezium
- Kafka Connect
- PostgreSQL Logical Replication
- Transactional Outbox 比較

## Flink

- Flink SQL
- Event Time
- Watermark
- Late Event
- Tumbling Window
- Stream Join
- Checkpoint

## OLAP / BI

- ClickHouse
- ODS / DWD / DWS / ADS
- Realtime GMV
- Payment Success Rate
- Dashboard

## Observability

- Prometheus
- Grafana
- Kafka JMX
- Consumer Lag Alert
- DLQ Rate Alert

## Kafka HA

- 3 Controllers
- 3 Brokers
- Replication Factor 3
- `min.insync.replicas=2`
- `acks=all`
- Broker Failover
- ISR Recovery

---

# 21. Codex 協作規則

每個 Phase 開始前，Codex 必須：

1. 閱讀 `AGENTS.md` 與本文件對應 Phase。
2. 檢查目前 Repository。
3. 摘要需求。
4. 列出新增或修改檔案。
5. 說明風險、假設與測試方式。
6. 等使用者確認後再修改（若使用者要求）。
7. 只實作指定 Phase。
8. 實際執行驗證命令。
9. 回報 Command、Exit Code、關鍵輸出與 Pass/Fail。
10. 更新測試與文件。
11. 停在當前 Phase，不自動開始下一階段。

---

# 22. 各 Phase 建議 Prompt

## Phase 1 As-built Review

```text
請先閱讀 AGENTS.md 與 SPEC.md。

現在只進行 Phase 1 as-built review，不要開始 Phase 2。
請檢查實作與規格差異，列出預計修改檔案與驗證計畫。
完成修正後，執行 Phase 1 Completion Gate，逐項回報 Command、Exit Code、關鍵輸出與 Pass/Fail。
```

## Phase 2

```text
請先閱讀 AGENTS.md 與 SPEC.md。

現在只執行 Phase 2：Event Models and Event Generator。
先摘要需求、列出檔案變更、測試計畫與風險，等我確認後再開始。
不要實作 Phase 3 Consumer。
```

## Phase 3

```text
請先閱讀 AGENTS.md 與 SPEC.md。

現在只執行 Phase 3：Order Consumer、Manual Offset Commit、Idempotency、Retry 與 DLQ。
先說明 Transaction Boundary、Offset Commit 時機、Duplicate Flow、DLQ Flow 與 Failure Scenarios，等我確認後再開始。
不要開始 Log Consumer。
```

## Phase 4

```text
請先閱讀 AGENTS.md 與 SPEC.md。

現在只執行 Phase 4：Application Log Consumer 與分鐘聚合。
先說明 Aggregation Key、Flush Strategy、Offset Strategy、Late Event 行為、檔案變更與測試案例，等我確認後再開始。
不要加入 Flink。
```

## Phase 5

```text
請先閱讀 AGENTS.md 與 SPEC.md。

現在只執行 Phase 5：Benchmark、Consumer Lag、Demo 與文件。
不得填入未實測的效能數據。
先列出 Benchmark 方法、指標、Failure Demo、README 更新範圍與預計修改檔案，等我確認後再開始。
```
