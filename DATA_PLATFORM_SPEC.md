# Retail Data Platform and AI Harness — Extension Specification

## 0. 文件定位

本文件是 `kafka-order-event-platform` 在完成 Kafka MVP 後，Phase 6～10 的功能規格來源。

本文件：

- 建立在 `SPEC.md` 所定義的 Phase 1～5 Kafka Event Processing Core 之上。
- 不取代 `SPEC.md`。
- 不得默默修改 Phase 1～5 已建立的 Kafka reliability semantics。
- 只允許實作使用者明確指定的 Phase。
- 每一個 Phase 都必須有明確 scope、non-goals、deliverables、acceptance criteria 與 completion gate。
- 尚未實作的功能不得在 README、履歷或面試說明中描述為已完成。

規格優先順序：

1. 使用者當次明確指令
2. 當前 Phase 對應的規格文件
3. `AGENTS.md`
4. 既有 repository conventions

Phase 路由：

| Phase | Track | 規格來源 |
|---|---|---|
| Phase 1～5 | Kafka Event Processing Core | `SPEC.md` |
| Phase 6～10 | Data Platform and AI Harness | `DATA_PLATFORM_SPEC.md` |
| 跨 Track 修改 | Cross-cutting | 同時閱讀兩份規格 |

若工作未明確指定 Phase，Codex 只能：

- 閱讀 repository
- 分析現況
- 列出差距
- 提出修改計畫
- 說明風險與測試方式

不得自行開始實作。

---

# 1. Extension 概述

## 1.1 Extension 名稱

```text
Retail Data Platform and AI Harness
```

## 1.2 一句話說明

> 在既有 Kafka 訂單事件平台之上，建立可重複使用的 dbt 資料開發 Paved Road、資料品質與 Data Contract、Slim CI、BigQuery 成本治理、metadata/lineage、唯讀 MCP Server，以及由 Codex Skills 驅動的資料開發與事件診斷流程。

## 1.3 作品集目標

完成必做主線 Phase 6、7、8A、9、10 後，本專案應能證明：

- 能將 event-processing system 延伸為 analytics data platform。
- 能設計 staging / intermediate / marts 的 dbt 分層。
- 能定義資料模型 grain、business semantics 與 Data Contract。
- 能建立 freshness、completeness、uniqueness、accepted values 等資料品質檢查。
- 能以 CI/CD 限制 breaking schema change 與不合規的 dbt model。
- 能使用 state-based selection 只驗證修改模型及其 downstream。
- 能以 PostgreSQL 提供完整本機主線，並以 BigQuery compatibility、Sandbox 與完整雲端三個層級逐步驗證。
- 能在 Phase 8A 定義並測試 BigQuery partition、clustering 與成本規則；只有 Phase 8C 完成後才能宣稱真實 incremental 與 cloud dry-run 已驗證。
- 能在本機驗證 Airflow DAG、retry、timeout 與 quality gate；只有 Phase 8C 完成後才能宣稱真實 BigQuery orchestration 已執行。
- 能從 dbt artifacts 建立 metadata、schema 與 lineage 查詢能力。
- 能開發受限制、唯讀且可稽核的 MCP tools。
- 能透過 Codex Skills 標準化 dbt scaffold 與 PR review。
- 能讓 Incident Diagnosis Agent 依證據查詢 log、lag、lineage 與品質結果。
- 能明確區分 deterministic tooling、LLM reasoning 與需要人工核准的修改操作。
- 能誠實說明本機作品集與 production platform 的差異。

## 1.4 不得宣稱

即使 Phase 6～10 完成，也不得宣稱：

- 整個系統已 production-ready。
- 本機 PostgreSQL 與單節點 Kafka 代表正式環境容量。
- MCP 或 Agent 可以安全地自主操作 production。
- AI 產生的 SQL 一定正確。
- 本專案已提供完整 enterprise data governance。
- 本專案已取代 Dataplex、Data Catalog、OpenLineage 或正式 Observability Platform。
- 本專案具備 autonomous remediation。
- 本專案具備跨 Kafka、PostgreSQL 與 BigQuery 的 exactly-once。
- 本機測得的 BigQuery 成本或效能可直接外推至企業規模。

---

# 2. 現有 As-built Baseline

## 2.1 Kafka Core Baseline

Phase 6～10 必須將 Phase 1～5 視為穩定 baseline。

下列行為不得未經明確核准而改變：

- Topic names
- Partition counts
- Message keys
- Consumer group names
- Event schema version
- Manual offset commit timing
- Idempotency key
- Database transaction boundary
- Retry classification
- Retry attempt count
- Retry backoff behavior
- DLQ delivery semantics
- Graceful shutdown behavior
- Benchmark definitions
- Consumer lag definitions

目前穩定 Topic：

```text
ecommerce.orders.raw.v1
ecommerce.application-logs.raw.v1
ecommerce.dlq.v1
```

目前穩定 Consumer Groups：

```text
order-processing-group-v1
application-log-processing-group-v1
```

## 2.2 現有 PostgreSQL Source Tables

Phase 6 的 dbt models 必須以現有資料表為起點，不可假設不存在的 raw table。

### `valid_orders`

Grain：

```text
one row per successfully persisted order event
```

Primary key：

```text
event_id
```

目前可用欄位：

- `event_id`
- `order_id`
- `event_type`
- `user_id`
- `product_id`
- `quantity`
- `amount`
- `currency`
- `channel`
- `event_time`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `created_at`

重要限制：

- 這張表保存的是 order event，不是 one row per order。
- 不同 event type 的 payload 欄位不同，因此部分欄位可以為 null。
- 目前沒有保存 `payment_id`、`payment_method`、`failure_code`、`failure_reason`、`cancellation_reason`。
- dbt model 不得捏造或推論未保存的欄位。
- 若未來需要保留完整 payload，必須另外提出 schema evolution 規格，不得在 Phase 6 默默修改。

目前支援的 Order Event Types：

```text
order_created
order_paid
order_cancelled
payment_failed
```

### `processed_events`

Grain：

```text
one row per consumer_group and processed event_id
```

Primary key：

```text
consumer_group, event_id
```

目前可用欄位：

- `consumer_group`
- `event_id`
- `topic`
- `partition_id`
- `offset_id`
- `processed_at`

用途：

- Kafka consumer idempotency marker
- operational metadata
- replay / processing evidence

限制：

- 此表不是 business fact table。
- 此表不得被 dbt transformation 修改。
- Analytics model 可以唯讀使用，但不可改變其 idempotency semantics。

### `log_metrics_minute`

Grain：

```text
one row per metric_minute, service and endpoint
```

Primary key：

```text
metric_minute, service, endpoint
```

目前可用欄位：

- `metric_minute`
- `service`
- `endpoint`
- `request_count`
- `success_count`
- `client_error_count`
- `server_error_count`
- `response_time_sum_ms`
- `max_response_time_ms`
- `updated_at`

重要限制：

- 目前 PostgreSQL 沒有保存原始 `api_access_log` rows。
- 目前只有 minute-level aggregate。
- 不得建立名稱或文件暗示 model 內含 raw application logs。
- 不得從 aggregate 反推 client IP、request ID、HTTP method 或 individual request latency。
- `average_response_time_ms` 應由 `response_time_sum_ms / request_count` 安全計算，不另行假設 source 欄位。

## 2.3 Source Ownership

Phase 1～5 components 擁有 source table 的寫入權。

Data Platform Track：

- 對 `public.valid_orders` 唯讀。
- 對 `public.processed_events` 唯讀。
- 對 `public.log_metrics_minute` 唯讀。
- 只可寫入獨立 analytics schema。
- 不可透過 dbt hook、macro 或 test 修改 source table。
- 不可刪除、truncate 或 rebuild source table。

---

# 3. Scope Boundaries

## 3.1 包含

Phase 6～10 包含：

- dbt project
- PostgreSQL local warehouse target
- staging / intermediate / marts
- source declarations
- model and column documentation
- model contracts
- data tests
- unit tests
- source freshness
- data-product metadata
- owner and SLO metadata
- generated dbt docs
- model scaffolding
- CI/CD
- Slim CI
- state-based model selection
- contract change detection
- BigQuery development target
- partitioning
- clustering
- incremental models
- BigQuery dry-run
- query bytes estimation
- cost guardrails
- Airflow orchestration
- idempotent PostgreSQL-to-BigQuery sync
- quality gates
- dbt manifest/catalog/run results processing
- metadata index
- lineage queries
- read-only MCP Server
- Codex repository-local Skills
- dbt scaffold workflow
- dbt PR review workflow
- Incident Diagnosis Agent workflow
- machine-readable quality and incident reports
- reproducible local demo
- optional real GCP evidence run

## 3.2 不包含

除非另立規格並取得明確核准，Phase 6～10 不包含：

- Flink
- Spark
- Debezium
- Kafka Connect
- Schema Registry
- ClickHouse
- Kubernetes
- Terraform
- Full Cloud Composer deployment automation
- Full Dataplex deployment
- Full OpenLineage backend
- Full-featured web UI
- BI dashboard application
- Semantic layer product
- dbt Mesh multi-project deployment
- Enterprise IAM platform
- Secrets manager deployment
- Multi-tenant control plane
- Model training
- Fine-tuning
- Vector database
- General-purpose RAG chatbot
- Multi-agent orchestration
- Autonomous production remediation
- Autonomous schema migration
- Autonomous Kafka offset reset
- Autonomous pipeline rerun
- Automatic PR merge
- Unrestricted SQL execution
- Arbitrary shell execution
- Arbitrary filesystem access
- Production SLA guarantee

## 3.3 Scope Change 規則

若實作中發現必須加入範圍外技術：

1. 先停止實作。
2. 說明為何現有設計無法完成需求。
3. 列出新增技術的必要性與替代方案。
4. 說明 dependency、security、operation 與 maintenance impact。
5. 更新本規格。
6. 取得使用者明確核准。
7. 才可開始實作。

---

# 4. Architecture Principles

## 4.1 Local-first

每一個 Phase 必須優先提供本機可重現路徑。

本機基本環境：

- Python 3.14.6
- pyenv virtualenv `kafka_streaming`
- Docker Compose
- Kafka
- PostgreSQL
- dbt-postgres
- pytest
- Ruff
- mypy

BigQuery、Airflow Provider、MCP SDK 或 AI API dependency：

- 必須放在清楚的 dependency group。
- 必須實際驗證 Python 3.14 compatibility。
- 不得因 dependency 不相容而默默降級 Python。
- 若確實不相容，必須先回報並提出隔離環境方案。

## 4.2 Stable Interfaces

Data Platform 只能透過穩定介面讀取 Kafka Core 產物：

- PostgreSQL source tables
- dbt artifacts
- machine-readable benchmark reports
- machine-readable consumer lag reports
- allowlisted structured logs

不得讓 analytics code 直接介入 Kafka poll loop 或 offset commit flow。

## 4.3 Layered Data Modeling

dbt layer 責任：

```text
sources
  → staging
  → intermediate
  → marts
```

### Sources

- 描述 upstream relation。
- 不進行 business transformation。
- 定義 freshness 與 source-level tests。
- 保存 source system naming。

### Staging

- 一個 source relation 對應一個主要 staging model。
- rename、cast、normalize。
- 不進行跨 source join。
- 不包含 consumer-facing business metrics。
- 不隱藏 source limitations。

### Intermediate

- 建立 reusable business logic。
- 可以 join、window、deduplicate 或 sequence。
- 名稱必須描述轉換意圖。
- 不直接作為外部穩定 data product。

### Marts

- 定義穩定 grain。
- 定義公開欄位與 business semantics。
- 必須有 owner、contract、tests 與 SLO。
- Breaking change 必須被 CI 偵測。
- 只暴露已驗證的資料，不暴露不必要的 operational columns。

## 4.4 Metadata-driven

下列能力必須盡量由 metadata 與 artifacts 驅動：

- model discovery
- schema discovery
- owner lookup
- lineage
- contract comparison
- impacted model selection
- quality status
- documentation completeness
- Agent evidence collection

不得將 model 清單硬編碼在多個不同工具。

## 4.5 Deterministic Before Generative

能以 deterministic code 完成的工作，優先使用 deterministic code：

- schema validation
- contract diff
- lineage traversal
- changed model detection
- SQL lint
- test execution
- query dry-run
- cost calculation
- quality status parsing

LLM / Codex 的責任：

- 理解需求
- 選擇工具
- 組織 context
- 解釋結果
- 提出可能原因
- 產生修改建議

LLM 不得取代可程式化驗證。

## 4.6 Read-only by Default

MCP tools 與 Incident Agent 預設唯讀。

任何 mutating operation 都必須具備：

- explicit user approval
- allowlisted operation
- validated parameters
- dry-run where possible
- auditable request and result
- rollback or recovery plan

Phase 9～10 第一版不得提供 production mutation tool。

## 4.7 Evidence and Honesty

任何驗收結果必須包含：

- command
- exit code
- relevant output
- pass / fail
- skipped reason
- execution environment
- timestamp

若沒有 GCP credentials：

- 必須清楚標示 cloud integration test 未執行。
- 不得以 unit test 冒充真實 BigQuery execution。
- 不得宣稱 BigQuery Phase 已完整驗收。

---

# 5. Target Architecture

## 5.1 Logical Architecture

```text
                        Phase 1–5 Kafka Core
┌──────────────────────────────────────────────────────────────┐
│ Event Generator                                               │
│     │                                                         │
│     ├── ecommerce.orders.raw.v1                               │
│     └── ecommerce.application-logs.raw.v1                     │
│               │                                               │
│        Reliable Consumers                                     │
│ manual commit / retry / idempotency / DLQ                    │
│               │                                               │
│        PostgreSQL public schema                               │
│        ├── valid_orders                                       │
│        ├── processed_events                                   │
│        └── log_metrics_minute                                 │
└──────────────────────────┬───────────────────────────────────┘
                           │ read-only
                           ▼
                    Phase 6 dbt Local
┌──────────────────────────────────────────────────────────────┐
│ dbt sources → staging → intermediate → marts                  │
│ contracts / tests / docs / freshness / SLO metadata          │
│ PostgreSQL analytics_<developer> schema                       │
└──────────────────────────┬───────────────────────────────────┘
                           │
               ┌───────────┴───────────┐
               ▼                       ▼
       Phase 7 Paved Road      Phase 8A Local BQ Policy
       scaffold / Slim CI      SQL rules / cost fixtures / DAG tests
               │                       │
               └───────────┬───────────┘
                           ▼
                  dbt artifacts and reports
         manifest / catalog / run_results / quality / cost
                           │
                           ▼
                   Phase 9 Metadata + MCP
            discovery / lineage / quality / lag / failures
                           │
                           ▼
                 Phase 10 Codex AI Harness
        dbt-scaffold / dbt-pr-review / incident-diagnosis
```

## 5.2 Target Repository Structure

Phase 實作可依 repository 現況做最小調整，但最終責任邊界應接近：

```text
kafka-order-event-platform/
├── AGENTS.md
├── SPEC.md
├── DATA_PLATFORM_SPEC.md
├── README.md
├── Makefile
├── pyproject.toml
├── docker-compose.yml
├── .env.example
├── .github/
│   └── workflows/
│       └── data-platform-ci.yml
├── .agents/
│   └── skills/
│       ├── dbt-scaffold/
│       │   ├── SKILL.md
│       │   ├── scripts/
│       │   ├── templates/
│       │   └── references/
│       ├── dbt-pr-review/
│       │   ├── SKILL.md
│       │   ├── scripts/
│       │   └── references/
│       └── incident-diagnosis/
│           ├── SKILL.md
│           ├── references/
│           └── fixtures/
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml.example
│   ├── selectors.yml
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
├── orchestration/
│   ├── dags/
│   └── include/
├── apps/
│   ├── data_discovery_mcp/
│   └── incident_agent/
├── src/
│   ├── streaming_platform/
│   └── data_platform/
│       ├── artifacts/
│       ├── contracts/
│       ├── lineage/
│       ├── metadata/
│       ├── mcp/
│       ├── quality/
│       └── incidents/
├── scripts/
│   └── data_platform/
│       ├── load_ci_fixtures.py
│       ├── scaffold_dbt_model.py
│       ├── compare_contracts.py
│       ├── estimate_bigquery_cost.py
│       └── build_metadata_index.py
├── tests/
│   └── data_platform/
│       ├── unit/
│       ├── integration/
│       └── e2e/
├── docs/
│   └── data-platform/
└── reports/
    ├── data-quality/
    ├── cost/
    ├── metadata/
    └── incidents/
```

原有 Phase 1～5 目錄與執行方式應維持相容。

---

# 6. Shared Naming and Modeling Standards

## 6.1 dbt Model Naming

| Layer | Prefix | Example |
|---|---|---|
| Staging | `stg_` | `stg_order_events` |
| Intermediate | `int_` | `int_order_event_sequence` |
| Fact | `fct_` | `fct_orders` |
| Dimension | `dim_` | 未來若有穩定 dimension 再建立 |
| Business Mart | `mart_` | `mart_daily_sales` |
| Snapshot | `snap_` | 僅在明確需要 SCD 時建立 |

不得建立：

- `final_table`
- `temp_data`
- `new_model`
- `test_model`
- 無法表達 grain 或用途的名稱

## 6.2 Column Naming

時間欄位：

- event business time：`*_at` 或保留清楚的 `event_time`
- calendar date：`*_date`
- UTC timestamp 必須 timezone-aware
- 不使用模糊名稱 `date`、`time`

Boolean：

```text
is_*
has_*
```

Counts：

```text
*_count
```

Amounts：

```text
*_amount
```

Rates：

```text
*_rate
```

Duration：

```text
*_ms
*_seconds
```

## 6.3 Grain Documentation

每個 intermediate 與 mart model 的 description 第一段必須明確寫出 grain，例如：

```text
Grain: one row per order_id.
```

或：

```text
Grain: one row per metric_minute, service and endpoint.
```

若 grain 無法明確說明，model 不得視為完成。

## 6.4 Monetary Rules

- PostgreSQL 使用 `numeric`。
- Python 使用 `Decimal`。
- dbt 不得將 money cast 成 floating-point 作為正式輸出。
- currency 必須保留。
- 不同 currency 不得直接相加為單一總額。
- `mart_daily_sales` 必須以 currency 分組，除非未來明確加入 FX conversion。

## 6.5 Null Semantics

因 `valid_orders` 包含多種 event type：

- `product_id`、`quantity`、`channel` 只保證在 `order_created` 有值。
- `amount`、`currency` 在 `order_created`、`order_paid`、`payment_failed` 預期有值。
- `order_cancelled` 的 amount 可以為 null。
- 測試必須使用 conditional validation，不可對整張 source 強制所有欄位 not null。
- mart contract 必須清楚區分 nullable 與 non-nullable。

## 6.6 Model Materialization

Phase 6 預設：

- staging：`view`
- intermediate：`ephemeral` 或 `view`，由可讀性與除錯需求決定
- small marts：`table`
- 需要增量與成本控制的 marts：只有 Phase 8C 才要求真實 BigQuery `incremental`；Phase 8A 僅驗證設計與政策

不得過早為展示技術而將所有 model 改成 incremental。

---

# 7. Data Product Metadata Standard

所有 published marts 必須在 YAML `meta` 或等價 metadata 中定義：

```yaml
meta:
  owner: data-platform
  domain: commerce
  data_product: orders
  maturity: experimental
  contains_pii: false
  sla:
    freshness_minutes: 60
    availability: best_effort
  contract_policy: breaking_changes_blocked
```

最少欄位：

- `owner`
- `domain`
- `data_product`
- `maturity`
- `contains_pii`
- `sla.freshness_minutes`
- `sla.availability`
- `contract_policy`

允許的 maturity：

```text
experimental
beta
stable
deprecated
```

Phase 6 預設所有 marts 為：

```text
experimental
```

除非通過完整 Phase 驗收並有穩定 consumer，不得自行標示 `stable`。

---

# 8. Quality Dimensions and SLO

## 8.1 Quality Dimensions

### Freshness

資料是否在預期時間內更新。

### Completeness

應有資料是否缺失，例如：

- order event 核心欄位
- minute metric counts
- required mart columns

### Uniqueness

定義的 grain key 是否唯一。

### Validity

值是否符合 domain：

- accepted event types
- supported currency
- non-negative counts
- valid rates
- valid timestamp

### Consistency

跨欄位關係是否合理，例如：

```text
success_count + client_error_count + server_error_count = request_count
```

### Referential Integrity

published mart 的 key 是否可追溯至 staging/source。

## 8.2 Initial SLO Defaults

這些是作品集 demo 的初始值，可由環境變數或 YAML 調整：

| Asset | Freshness warn | Freshness error | Key quality target |
|---|---:|---:|---|
| `stg_order_events` | 30 minutes | 120 minutes | event_id uniqueness = 100% |
| `stg_log_metrics_minute` | 30 minutes | 120 minutes | composite key uniqueness = 100% |
| `fct_orders` | 45 minutes | 180 minutes | order_id uniqueness = 100% |
| `mart_daily_sales` | 60 minutes | 240 minutes | required fields complete = 100% |
| `mart_service_health` | 45 minutes | 180 minutes | count consistency = 100% |

注意：

- 本機 demo 不是 24/7 system。
- Freshness 驗收前必須先載入當次 fixture 或產生 smoke events。
- 不得因長時間未執行本機環境而將過期資料誤稱為 pipeline failure。
- CI fixture 必須使用接近執行當下的 deterministic timestamp。

---

# 9. Phase 6 — dbt Data Products on PostgreSQL

## 9.1 目標

以現有 PostgreSQL tables 為唯讀 sources，建立可在本機完整驗證的 dbt project，展示：

- 分層建模
- grain 定義
- documentation
- data contracts
- data tests
- unit tests
- source freshness
- generated docs
- data-product metadata

Phase 6 不引入 BigQuery、Airflow、MCP 或 Agent。

## 9.2 Scope

Phase 6 包含：

- `dbt/` project
- dbt-postgres
- PostgreSQL analytics schema
- source declarations
- staging models
- intermediate models
- marts
- contracts
- tests
- docs
- Makefile commands
- local fixture generation
- Phase 6 documentation

## 9.3 Non-goals

Phase 6 不包含：

- GitHub Actions Slim CI
- BigQuery
- Airflow
- query cost dry-run
- MCP Server
- Codex Skills implementation
- Agent implementation
- source schema change
- raw application log persistence
- BI dashboard
- production scheduler

## 9.4 Target and Schema Isolation

dbt profile 最少提供：

```text
local
ci
```

本機 target：

```text
database: existing PostgreSQL database
source schema: public
target schema: analytics_<developer>
```

CI target：

```text
source schema: public
target schema: analytics_ci_<run_identifier>
```

規則：

- 不可將 dbt models 寫回 `public`。
- 不可硬編碼個人帳號。
- schema 名稱必須可由環境變數設定。
- credentials 只能由 environment variables 提供。
- repository 只能提交 `profiles.yml.example`，不得提交真實密碼。

## 9.5 Required Sources

`dbt/models/staging/sources.yml` 至少定義：

```text
source: streaming_platform
schema: public

tables:
- valid_orders
- processed_events
- log_metrics_minute
```

Source tests：

### `valid_orders`

- `event_id` not null
- `event_id` unique
- `order_id` not null
- `event_type` not null
- `user_id` not null
- `event_time` not null
- `kafka_topic` not null
- `kafka_partition` not null
- `kafka_offset` not null
- accepted event types
- amount non-negative when present
- quantity positive when present

### `processed_events`

- composite uniqueness：`consumer_group`, `event_id`
- topic not null
- partition_id non-negative
- offset_id non-negative
- processed_at not null

### `log_metrics_minute`

- composite uniqueness：`metric_minute`, `service`, `endpoint`
- request_count non-negative
- each status count non-negative
- response_time_sum_ms non-negative
- max_response_time_ms non-negative
- status counts sum to request_count

## 9.6 Required Staging Models

### `stg_order_events`

Source：

```text
public.valid_orders
```

Grain：

```text
one row per event_id
```

責任：

- 保留 source event grain。
- 統一 naming。
- 明確 cast type。
- 將 timestamp 保持 UTC。
- 不做 one-row-per-order 聚合。
- 不捏造未保存 payload 欄位。
- 保留 Kafka source coordinates 供追蹤。
- 計算必要的 event date。

建議輸出欄位：

- `event_id`
- `order_id`
- `event_type`
- `user_id`
- `product_id`
- `quantity`
- `amount`
- `currency`
- `channel`
- `event_time`
- `event_date`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `persisted_at`

### `stg_processed_events`

Source：

```text
public.processed_events
```

Grain：

```text
one row per consumer_group and event_id
```

責任：

- 提供 operational processing metadata。
- rename `partition_id` / `offset_id` 為一致的 Kafka naming。
- 不將此表解釋為 business event completeness。

建議輸出欄位：

- `consumer_group`
- `event_id`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `processed_at`

### `stg_log_metrics_minute`

Source：

```text
public.log_metrics_minute
```

Grain：

```text
one row per metric_minute, service and endpoint
```

責任：

- 保留 minute aggregate grain。
- 安全計算 `average_response_time_ms`。
- request_count 為 0 時 average 必須為 null 或明確定義值，不可除以零。
- 不得暗示含 raw request rows。

建議輸出欄位：

- `metric_minute`
- `metric_date`
- `service`
- `endpoint`
- `request_count`
- `success_count`
- `client_error_count`
- `server_error_count`
- `response_time_sum_ms`
- `average_response_time_ms`
- `max_response_time_ms`
- `updated_at`

## 9.7 Required Intermediate Models

### `int_order_event_sequence`

Grain：

```text
one row per event_id, enriched with per-order event sequence
```

責任：

- 依 `order_id` 建立 deterministic event sequence。
- 同一 `order_id` 預期因 Kafka key 而位於同一 partition。
- 排序優先使用 Kafka stream order，並保留 event time 供 business-time 分析。
- 提供 previous / next event type。
- 提供 event sequence number。
- 提供從建立到目前 event 的 elapsed time。

建議輸出欄位：

- staging 全部核心欄位
- `event_sequence_number`
- `previous_event_type`
- `next_event_type`
- `previous_event_time`
- `seconds_since_previous_event`
- `first_event_time`
- `latest_event_time`

必要測試：

- `event_id` unique
- sequence number positive
- 同一 order 的 Kafka partition 一致
- sequence deterministic
- no duplicated Kafka source coordinate within the topic

### `int_order_latest_state`

Grain：

```text
one row per order_id
```

責任：

- 找出每個 order 的最新已知 event。
- 保留建立事件的 product、quantity、channel 與 original amount。
- 保留最近 payment event amount。
- 不推論不存在的 payment details。
- 將「latest known state」與「business final state」分開描述。

建議 state mapping：

| Latest event type | `latest_order_state` |
|---|---|
| `order_created` | `created` |
| `order_paid` | `paid` |
| `order_cancelled` | `cancelled` |
| `payment_failed` | `payment_failed` |

注意：

- 這是目前事件集合的 latest known state。
- `payment_failed` 後未來仍可能出現 `order_paid`。
- 不得將 `payment_failed` 永久視為 final state。
- 不得假設 event_time 完全無延遲；應保留 sequence basis 文件。

### `int_service_minute_metrics`

Grain：

```text
one row per metric_minute, service and endpoint
```

責任：

- 提供安全 rate 計算。
- 保留原始 counts。
- 將 rate 限制在 0～1。
- 清楚處理 request_count = 0。

建議欄位：

- staging 全部欄位
- `success_rate`
- `client_error_rate`
- `server_error_rate`
- `error_rate`

## 9.8 Required Marts

### `fct_order_events`

Grain：

```text
one row per order event_id
```

Purpose：

- 提供乾淨、穩定且有 contract 的 order event fact。
- 保留 Kafka lineage columns。
- 作為 event-level analysis 與其他 marts 的穩定基礎。

必須有：

- model contract
- owner metadata
- all columns documented
- event_id unique
- event_id not null
- accepted event types
- conditional field tests

### `fct_orders`

Grain：

```text
one row per order_id
```

Purpose：

- 提供每個 order 的 latest known lifecycle state。
- 聚合建立、付款、取消與付款失敗的時間與狀態。

建議欄位：

- `order_id`
- `user_id`
- `product_id`
- `quantity`
- `currency`
- `channel`
- `order_created_at`
- `first_paid_at`
- `latest_payment_failed_at`
- `cancelled_at`
- `latest_event_at`
- `latest_event_type`
- `latest_order_state`
- `original_order_amount`
- `latest_paid_amount`
- `payment_attempt_count`
- `payment_failure_count`
- `is_paid`
- `is_cancelled`

規則：

- `order_id` unique。
- `user_id` 不得跨事件不一致；若不一致，quality test 必須失敗。
- 不同 currency 的 events 不得被無聲合併。
- `is_paid` 代表至少出現一次 `order_paid`，不是 accounting settlement 保證。
- `is_cancelled` 代表至少出現一次 `order_cancelled`。
- 若同時 paid 與 cancelled，保留兩個 flags，latest state 依最後事件決定。

### `mart_daily_sales`

Grain：

```text
one row per event_date, currency and channel
```

Purpose：

- 提供 daily order creation、payment、cancellation 與 payment failure 指標。
- 避免將不同 currency 直接相加。

建議欄位：

- `event_date`
- `currency`
- `channel`
- `created_order_count`
- `created_order_amount`
- `paid_order_count`
- `paid_amount`
- `cancelled_order_count`
- `payment_failed_order_count`
- `payment_failed_amount`
- `payment_attempt_count`
- `payment_success_rate`

Metric 定義：

```text
created_order_count
= distinct order_id with order_created

created_order_amount
= sum amount on order_created

paid_order_count
= distinct order_id with order_paid

paid_amount
= sum amount on order_paid

payment_attempt_count
= count of order_paid + payment_failed events

payment_success_rate
= order_paid event count / payment_attempt_count
```

限制：

- `paid_amount` 不等同正式財務認列 revenue。
- 沒有 refund event，不能計算 net revenue。
- 沒有 FX rate，不能輸出跨 currency consolidated GMV。

### `mart_service_health`

Grain：

```text
one row per metric_minute and service
```

Purpose：

- 將 endpoint minute metrics 聚合為 service-level health view。

建議欄位：

- `metric_minute`
- `metric_date`
- `service`
- `request_count`
- `success_count`
- `client_error_count`
- `server_error_count`
- `error_count`
- `success_rate`
- `error_rate`
- `weighted_average_response_time_ms`
- `max_response_time_ms`
- `endpoint_count`

規則：

- weighted average 必須使用 sum(response_time_sum_ms) / sum(request_count)。
- 不得直接 average endpoint-level averages。
- counts 必須保持一致。
- request_count = 0 時 rate 與 average 的行為必須明確。

## 9.9 Data Contracts

Phase 6 至少要求：

- `fct_order_events`
- `fct_orders`
- `mart_daily_sales`
- `mart_service_health`

具備 enforced model contract，若 adapter / materialization 支援。

若 adapter 限制導致無法 enforce：

- 不得直接省略。
- 必須提供 schema assertion test。
- 必須在文件中說明 adapter limitation。
- Phase 7 contract diff 仍必須可運作。

Contract 必須定義：

- column name
- data type
- nullable expectation
- description
- semantic meaning

## 9.10 Unit Tests

至少建立以下 dbt unit-test scenarios：

1. `order_created → order_paid`
   - latest state = paid
   - is_paid = true
   - payment_failure_count = 0

2. `order_created → payment_failed → order_paid`
   - latest state = paid
   - payment_attempt_count = 2
   - payment_failure_count = 1

3. `order_created → order_cancelled`
   - latest state = cancelled
   - is_cancelled = true

4. 同分鐘 service metrics
   - weighted average 正確
   - counts 正確加總

5. request_count = 0
   - 不發生 division by zero
   - rate / average 遵守明確規則

6. multi-currency
   - `mart_daily_sales` 不會將 TWD 與 USD 合併

## 9.11 Local Fixtures

Phase 6 必須提供 deterministic fixture path。

可以使用：

- dbt seeds
- Python fixture loader
- 既有 event generator 的 deterministic preset

Fixture 至少包含：

- 正常 paid order
- payment failed then paid order
- cancelled order
- duplicate event replay 已由 upstream idempotency 排除
- TWD 與 USD
- web / ios / android
- multiple services
- 2xx / 4xx / 5xx minute metrics
- request_count = 0 edge case，若 source constraints 允許

Fixture loader：

- 可安全重跑。
- 不重設 Kafka offsets。
- 不破壞既有 benchmark data。
- 使用獨立 run identifier 或明確資料範圍。
- 產生 machine-readable fixture summary。

## 9.12 Required Makefile Commands

命名可依現有 Makefile 風格微調，但至少提供等價指令：

```bash
make dbt-deps
make dbt-debug
make dbt-parse
make dbt-compile
make dbt-build
make dbt-test
make dbt-source-freshness
make dbt-docs
make data-platform-fixtures
make test-data-platform
```

所有命令必須：

- 使用 project virtualenv。
- 避免硬編碼使用者路徑。
- 非零 exit code 表示失敗。
- 失敗時保留可理解的錯誤訊息。

## 9.13 Phase 6 Acceptance Criteria

- [ ] dbt project 可由 clean checkout 安裝。
- [ ] `dbt debug` 成功連到本機 PostgreSQL。
- [ ] dbt 只讀取 `public` source tables。
- [ ] dbt 寫入獨立 analytics schema。
- [ ] required sources 全部定義。
- [ ] required staging models 全部存在。
- [ ] required intermediate models 全部存在。
- [ ] required marts 全部存在。
- [ ] 每個 intermediate / mart 有 grain。
- [ ] published marts 有 owner / SLO metadata。
- [ ] published marts 有完整 column descriptions。
- [ ] published marts 有 contract 或明確等價檢查。
- [ ] source tests 通過。
- [ ] data tests 通過。
- [ ] unit tests 通過。
- [ ] freshness 可在新 fixture 後通過。
- [ ] dbt docs 可產生。
- [ ] README 不宣稱 BigQuery、MCP 或 Agent 已完成。
- [ ] Phase 1～5 完整 tests 仍通過。
- [ ] Kafka reliability semantics 未改變。

## 9.14 Phase 6 Completion Gate

```bash
make up
make topics
make migrate
make data-platform-fixtures
make dbt-deps
make dbt-debug
make dbt-parse
make dbt-compile
make dbt-build
make dbt-test
make dbt-source-freshness
make dbt-docs
make lint
make typecheck
make test
make test-data-platform
```

Completion report 必須逐項列出：

- Command
- Exit Code
- Key Output
- Pass / Fail
- Notes

完成後停止，不得自動開始 Phase 7。

---

# 10. Phase 7 — Paved Road and Slim CI

## 10.1 目標

將 Phase 6 的 dbt conventions 轉化為可重複使用的開發 Paved Road，讓新增 model 時能自動獲得：

- directory convention
- SQL template
- YAML template
- contract skeleton
- required tests
- owner / SLO metadata
- local validation
- CI validation

## 10.2 Scope

Phase 7 包含：

- model scaffold CLI
- reusable templates
- convention validation
- GitHub Actions
- CI PostgreSQL service
- deterministic CI fixtures
- dbt state artifacts
- Slim CI
- contract diff
- documentation checks
- machine-readable CI summary
- developer workflow documentation

## 10.3 Non-goals

Phase 7 不包含：

- BigQuery execution
- BigQuery cost dry-run
- Airflow
- MCP
- Codex Skill implementation
- Agent
- automatic code modification after review
- automatic PR approval
- automatic merge

## 10.4 Scaffold Interface

至少提供：

```bash
python scripts/data_platform/scaffold_dbt_model.py \
  --name daily_customer_orders \
  --layer marts \
  --owner data-platform \
  --domain commerce \
  --grain "one row per event_date and user_id"
```

或等價 Makefile：

```bash
make dbt-model \
  MODEL=daily_customer_orders \
  LAYER=marts \
  OWNER=data-platform \
  DOMAIN=commerce
```

Required inputs：

- model name
- layer
- owner
- domain
- grain

Optional inputs：

- materialization
- upstream models
- data product
- freshness target
- contains PII
- incremental key

Scaffold 不得：

- 猜測 source columns。
- 自動建立不存在的 source。
- 覆寫既有檔案。
- 產生沒有 grain 的 mart。
- 產生沒有 owner 的 published mart。
- 產生空白 contract 並宣稱完成。

## 10.5 Generated Files

依 layer 產生：

```text
dbt/models/<layer>/<model_name>.sql
dbt/models/<layer>/<model_name>.yml
```

必要時產生：

```text
dbt/tests/<model_name>__*.sql
dbt/models/<layer>/unit_tests.yml
```

SQL template 至少包含：

- model config
- upstream `ref()` placeholder
- CTE structure
- final select
- TODO marker，且 TODO 不得被誤認為完成

YAML template 至少包含：

- model description
- grain
- owner
- domain
- maturity
- SLO metadata
- contract skeleton
- column descriptions
- required tests

## 10.6 Convention Validator

必須以 deterministic script 驗證：

- model path 與 prefix 一致
- mart 有 grain
- mart 有 owner
- mart 有 contract
- required columns 有 description
- prohibited model names
- direct source reference 僅允許 staging
- marts 不直接使用 `source()`
- staging 不跨 source join
- wildcard `select *` policy
- model metadata 完整
- no duplicate model names

Validator 結果區分：

```text
error
warning
info
```

Error 必須使 CI 失敗。

## 10.7 Slim CI

CI 必須使用 state comparison。

最少流程：

```text
checkout
→ install dependencies
→ start PostgreSQL service
→ run migrations
→ load deterministic fixtures
→ obtain previous manifest
→ dbt deps
→ dbt parse
→ convention validation
→ contract comparison
→ dbt build --select state:modified+
→ documentation validation
→ existing Python checks
→ publish artifacts
```

若沒有 previous manifest：

- 清楚標示 full CI fallback。
- 執行完整 `dbt build`。
- 不得靜默跳過。

State artifacts 至少包含：

- `manifest.json`
- `catalog.json`，若有產生
- `run_results.json`
- sources / freshness results，若有
- contract diff report
- CI summary

## 10.8 Contract Change Policy

Breaking changes：

- remove published column
- rename published column
- change data type incompatibly
- change nullable to required without migration plan
- change model grain
- change primary business key
- change metric semantic without versioning
- remove contract
- remove required test

Potentially breaking：

- add required column
- change owner
- shorten freshness SLO
- change materialization
- change incremental unique key

Non-breaking：

- add nullable documented column
- add tests
- improve description
- internal CTE refactor without output change

CI 對 breaking change：

- 預設 blocking。
- 必須輸出 affected model、column、change type 與 downstream impact。
- 不得只顯示 generic failure。

## 10.9 Required CI Scenarios

### Scenario A — Normal New Model

- 新增符合規範的 model。
- CI 通過。
- 只建置新 model 與必要 downstream。

### Scenario B — Removed Contract Column

- 從 published mart 移除欄位。
- Contract diff 偵測。
- CI 失敗。
- Report 明確說明 breaking change。

### Scenario C — Upstream Change

- 修改 staging model。
- CI 選取 modified model 與 downstream。
- 未受影響 model 不必全部重跑。
- Selection evidence 必須可查看。

### Scenario D — Missing Documentation

- 新增 mart column 但無 description。
- CI 失敗或依 policy blocking。

### Scenario E — Invalid Scaffold Request

- model 已存在。
- Scaffold 必須拒絕覆寫。
- 回傳非零 exit code。

## 10.10 Adoption Metrics

Phase 7 可產生簡單 adoption / developer experience metrics：

- scaffold invocation count in demo
- generated files count
- validation duration
- selected model count
- full build model count
- Slim CI model count
- CI duration
- contract findings count

限制：

- 不宣稱這些數據代表真實多人團隊 adoption。
- 只能描述本 repository 的 demo runs。

## 10.11 Phase 7 Acceptance Criteria

- [ ] Scaffold CLI 可重現。
- [ ] Scaffold 不覆寫既有檔案。
- [ ] Mart template 包含 grain、owner、contract、tests。
- [ ] Convention validator 可執行。
- [ ] GitHub Actions workflow 存在。
- [ ] CI 使用 PostgreSQL service 與 deterministic fixtures。
- [ ] State artifact 可保存與使用。
- [ ] Slim CI 可辨認 modified models。
- [ ] Breaking contract change 會阻擋。
- [ ] Missing documentation 會被偵測。
- [ ] CI 產生 machine-readable summary。
- [ ] Phase 6 local path 仍可使用。
- [ ] Phase 1～5 tests 仍通過。

## 10.12 Phase 7 Completion Gate

```bash
make data-platform-fixtures
make dbt-scaffold-smoke
make dbt-validate-conventions
make dbt-contract-check
make dbt-slim-ci-local
make lint
make typecheck
make test
make test-data-platform
```

另外必須提供 GitHub Actions evidence：

- workflow run URL 或可驗證的 run summary
- passed scenario evidence
- failed breaking-change scenario evidence

完成後停止，不得自動開始 Phase 8A。

---

# 11. Phase 8 — BigQuery Compatibility, Sandbox Validation and Optional Full Cloud

## 11.1 Phase 結構

Phase 8 拆成三個完成層級：

| Subphase | 名稱 | Requirement | 是否需要 Billing |
|---|---|---|---|
| Phase 8A | Local BigQuery Compatibility and Cost Policy | 必做 | 否 |
| Phase 8B | BigQuery Sandbox Validation | 選做 | 否 |
| Phase 8C | Full BigQuery Pipeline | 延後選做 | 是 |

作品集必做主線為：

```text
Phase 6
→ Phase 7
→ Phase 8A
→ Phase 9
→ Phase 10
```

Phase 8B 與 Phase 8C：

- 不阻擋 Phase 9。
- 不阻擋 Phase 10。
- 不阻擋本機作品集主線完成。
- 不得因尚未完成而將整個專案標示為 blocked。
- 只有實際通過各自 acceptance criteria，才可在 README 或面試中宣稱已完成。

## 11.2 BigQuery Sandbox Constraints

截至 `2026-08-06`，BigQuery Sandbox 的官方限制包括：

- 不需要提供信用卡。
- 不需要為專案建立 Billing Account。
- 免費使用額度與 BigQuery free tier 相同：
  - 每月 10 GB active storage。
  - 每月 1 TB processed query data。
- Sandbox datasets 具有預設 table expiration。
- tables、views 與 partitions 預設 60 天後到期。
- 不支援 streaming data。
- 不支援 DML statements。
- 不支援 BigQuery Data Transfer Service。

這些屬於外部平台限制，可能在未來改變。

開始 Phase 8B 前必須：

1. 重新查閱 Google Cloud 官方 BigQuery Sandbox 文件。
2. 記錄確認日期。
3. 記錄當時適用的限制。
4. 若官方限制與本文件不同，更新本章後才執行。
5. 不得依賴非官方部落格作為限制來源。

由於 Sandbox 不支援 DML：

- Phase 8B 不要求 `MERGE`。
- Phase 8B 不要求 incremental dbt model 實際執行。
- Phase 8B 不要求 PostgreSQL-to-BigQuery upsert。
- Phase 8B 不要求 streaming ingestion。
- Phase 8B 不要求 Cloud Composer。
- Phase 8B 不得宣稱已驗證 production-like BigQuery pipeline。

---

## 11.3 Shared Phase 8 Principles

### 11.3.1 PostgreSQL Remains the Reproducible Baseline

Phase 6 PostgreSQL dbt path 必須保持：

- 本機可重現。
- 不依賴 GCP credentials。
- 不依賴 Billing。
- 不依賴 Sandbox project。
- 可執行完整 dbt build、tests、contracts 與 docs。

任何 Phase 8 功能不得使下列命令失效：

```bash
make data-platform-fixtures
make dbt-build
make dbt-test
make test-data-platform
```

### 11.3.2 No Fake Cloud Claims

不得將下列結果描述為真實 BigQuery execution：

- static SQL parsing
- SQL lint
- regex-based partition-filter checks
- fixture-based dry-run responses
- mocked BigQuery client
- mocked Airflow tasks
- locally generated bytes estimates
- manually written example job IDs
- unit-test fixtures

這些結果必須標示為：

```text
simulated
static_validation
fixture_based
local_only
```

只有 Google Cloud 實際回傳的結果才可以標示：

```text
sandbox_observed
cloud_observed
```

### 11.3.3 Adapter Isolation

BigQuery-specific implementation 必須與 PostgreSQL baseline 隔離。

允許：

- adapter-specific dbt macros
- target-aware model config
- BigQuery-specific SQL models
- separate profiles target
- separate dependency group
- separate optional test marker

不得：

- 為了 BigQuery 破壞 PostgreSQL models。
- 在 common code 中硬編碼 GCP project ID。
- 將 cloud credentials 設為本機測試必要條件。
- 無 credentials 時讓全部 CI 失敗。
- 將 Sandbox capability 當作 Full Cloud capability。

### 11.3.4 Evidence Classification

Phase 8 reports 必須包含：

```json
{
  "evidence_level": "simulated",
  "provider": "local",
  "billing_enabled": false,
  "observed_job_id": null,
  "estimated_bytes": null,
  "source": "fixture",
  "generated_at": "ISO-8601 timestamp"
}
```

允許的 `evidence_level`：

```text
static_validation
simulated
sandbox_observed
cloud_observed
not_available
```

規則：

- `simulated` 不得包含假的 cloud job ID。
- `sandbox_observed` 必須包含真實 Sandbox job evidence。
- `cloud_observed` 必須包含 Billing-enabled project 的真實 job evidence。
- `not_available` 不得使用 `0` 偽裝沒有成本或沒有資料。
- 任何 report 都必須記錄產生時間與執行環境。

---

# 11A. Phase 8A — Local BigQuery Compatibility and Cost Policy

## 11A.1 目標

在完全不使用 GCP credentials、信用卡或 Billing 的前提下，建立一套可重現的 BigQuery compatibility 與 cost-governance Paved Road。

Phase 8A 必須證明：

- 知道 PostgreSQL 與 BigQuery SQL/config 的差異。
- 能設計 partition 與 clustering metadata。
- 能檢查 published mart 是否要求 partition filter。
- 能偵測高風險 SQL pattern。
- 能定義 query bytes threshold policy。
- 能處理 simulated dry-run reports。
- 能在本機驗證 Airflow DAG structure、retry、timeout 與 quality gate。
- 能誠實區分模擬與真實雲端 evidence。

## 11A.2 Scope

Phase 8A 包含：

- BigQuery-compatible model configuration
- adapter-aware macros
- BigQuery SQL static validation
- partition metadata rules
- clustering metadata rules
- `require_partition_filter` policy
- `SELECT *` detection
- partition predicate detection
- cross-currency safety checks
- join explosion warnings
- cost threshold configuration
- fixture-based dry-run result parser
- simulated cost reports
- Airflow DAG import tests
- Airflow task unit tests
- local orchestration plan
- quality-gate state machine
- README limitations
- machine-readable reports

## 11A.3 Non-goals

Phase 8A 不包含：

- 建立 GCP project
- 建立 Billing Account
- BigQuery API authentication
- 真實 BigQuery dataset
- 真實 BigQuery table
- 真實 BigQuery job
- 真實 bytes processed measurement
- `dbt-bigquery` cloud execution
- PostgreSQL-to-BigQuery load
- DML
- `MERGE`
- streaming
- Cloud Composer
- production scheduler
- production cost claim

## 11A.4 Suggested Repository Structure

```text
dbt/
├── macros/
│   ├── adapter/
│   │   ├── date_helpers.sql
│   │   ├── safe_divide.sql
│   │   └── partition_config.sql
│   └── governance/
│       ├── require_partition_filter.sql
│       └── currency_guard.sql
├── models/
│   └── marts/
│       └── properties.yml
└── targets/
    └── bigquery/
        └── README.md

orchestration/
├── dags/
│   └── retail_data_platform_validation.py
└── include/
    └── policies/

scripts/data_platform/
├── inspect_bigquery_sql.py
├── validate_partition_policy.py
├── validate_cost_policy.py
├── parse_dry_run_fixture.py
└── build_cost_report.py

tests/data_platform/
├── unit/
│   ├── test_bigquery_sql_policy.py
│   ├── test_partition_policy.py
│   ├── test_cost_policy.py
│   └── test_airflow_validation_dag.py
└── fixtures/
    └── bigquery_dry_run/
        ├── within_threshold.json
        ├── exceeds_threshold.json
        ├── invalid_query.json
        └── missing_estimate.json

reports/cost/
└── .gitkeep
```

實際路徑可依 repository conventions 微調，但責任必須分離。

## 11A.5 BigQuery Compatibility Contract

每個準備支援 BigQuery 的 published mart，必須宣告：

```yaml
meta:
  warehouse_compatibility:
    postgres: supported
    bigquery: planned
  bigquery:
    partition_by:
      field: event_date
      data_type: date
      granularity: day
    cluster_by:
      - currency
      - channel
    require_partition_filter: true
```

Phase 8A 的 `bigquery: planned` 代表：

- config 已定義。
- static policy 已驗證。
- 尚未在 BigQuery 執行。

完成 Phase 8B 後，可改為：

```text
sandbox_validated
```

只有完成 Phase 8C 後，可改為：

```text
cloud_validated
```

允許狀態：

```text
not_supported
planned
static_validated
sandbox_validated
cloud_validated
```

## 11A.6 Required Model Policies

### `mart_daily_sales`

預期 BigQuery metadata：

```yaml
partition_by:
  field: event_date
  data_type: date
  granularity: day

cluster_by:
  - currency
  - channel

require_partition_filter: true
```

規則：

- 所有 production-like consumer query 範例必須帶 `event_date` predicate。
- 不同 currency 不得合併。
- 不得將 static config 描述為已建立 BigQuery partition。

### `mart_service_health`

預期 BigQuery metadata：

```yaml
partition_by:
  field: metric_date
  data_type: date
  granularity: day

cluster_by:
  - service

require_partition_filter: true
```

規則：

- service health query 必須帶日期範圍。
- endpoint-level query 必須有 bounded time window。
- 不得對 entire history 提供無限制 scan 範例。

### `fct_order_events`

預期 BigQuery metadata：

```yaml
partition_by:
  field: event_date
  data_type: date
  granularity: day

cluster_by:
  - order_id
  - event_type

require_partition_filter: true
```

## 11A.7 SQL Policy Validator

必須使用 deterministic code 檢查：

- published model 是否定義 partition field。
- partition field 是否存在於 contract。
- partition field data type 是否合理。
- `require_partition_filter` 是否啟用。
- cluster columns 是否存在。
- cluster column 數量是否在 policy 範圍內。
- example queries 是否包含 partition predicate。
- 是否存在不必要 `SELECT *`。
- 是否存在未受限制的 cross join。
- 是否存在明顯 cartesian product。
- 是否在 money aggregation 中遺失 currency。
- 是否在 ratio 計算中未處理 division by zero。
- 是否對 partition column 套用可能阻止 pruning 的不必要 transformation。
- incremental example 是否缺少 bounded lookback 說明。

Validator 不得只使用單一 regex 判斷所有 SQL 語意。

允許組合：

- SQL parser
- dbt manifest
- compiled SQL
- configuration metadata
- bounded regex checks

若 parser 無法理解 SQL：

- 回傳 `unknown`。
- 不得直接回傳 pass。
- 可將 unknown 設為 blocking 或 manual review。

## 11A.8 Cost Policy Configuration

建議設定檔：

```yaml
schema_version: 1

default:
  maximum_bytes_processed: 1073741824
  action: block

models:
  mart_daily_sales:
    maximum_bytes_processed: 536870912
    action: block

  mart_service_health:
    maximum_bytes_processed: 268435456
    action: warn
```

數值是作品集 policy，不代表真實 production threshold。

每個 threshold 必須：

- 有單位。
- 有理由。
- 可透過環境變數或 config 覆寫。
- 不硬編碼在多個 scripts。
- 在 report 中顯示。
- 清楚標示本機 threshold 並非實際 BigQuery quota。

## 11A.9 Dry-run Fixture Schema

Phase 8A 使用 fixture 模擬 BigQuery dry-run response。

Fixture：

```json
{
  "schema_version": 1,
  "fixture_name": "within_threshold",
  "query_id": "mart_daily_sales_7d",
  "valid": true,
  "total_bytes_processed": 104857600,
  "total_bytes_billed": 0,
  "cache_hit": false,
  "error": null
}
```

Invalid fixture：

```json
{
  "schema_version": 1,
  "fixture_name": "invalid_query",
  "query_id": "mart_daily_sales_invalid",
  "valid": false,
  "total_bytes_processed": null,
  "total_bytes_billed": null,
  "cache_hit": false,
  "error": {
    "category": "invalid_query",
    "message": "Sanitized fixture error"
  }
}
```

Fixture rules：

- 檔名與內容明確寫出 `fixture`。
- 不使用 `job_id` 欄位，避免誤認為真實 job。
- 不使用真實 credentials。
- 不把 `total_bytes_billed = 0` 解釋成真實免費執行。
- missing estimate 不得自動填 0。
- parser 必須驗證 schema version。

## 11A.10 Simulated Cost Report

產出：

```text
reports/cost/local-cost-policy-report.json
reports/cost/local-cost-policy-report.md
```

JSON 最少包含：

```json
{
  "report_type": "bigquery_cost_policy",
  "schema_version": 1,
  "evidence_level": "simulated",
  "provider": "local",
  "billing_enabled": false,
  "query_id": "mart_daily_sales_7d",
  "estimated_bytes": 104857600,
  "threshold_bytes": 536870912,
  "decision": "pass",
  "source_fixture": "within_threshold.json",
  "observed_job_id": null,
  "warnings": [
    "This report is fixture-based and is not a real BigQuery measurement."
  ],
  "generated_at": "ISO-8601 timestamp"
}
```

Decision：

```text
pass
warn
block
invalid
unknown
```

## 11A.11 Local Airflow Validation DAG

Phase 8A 可以加入 Airflow，但用途只限本機 workflow validation。

建議 DAG ID：

```text
retail_data_platform_validation
```

Logical flow：

```text
validate_environment
→ dbt_parse
→ dbt_compile
→ validate_model_conventions
→ validate_partition_policy
→ validate_cost_policy
→ quality_gate
→ publish_local_reports
```

不得包含宣稱會實際執行的：

- BigQuery load
- BigQuery MERGE
- Cloud Composer deployment
- production publish

DAG requirements：

- import 無 network call。
- import 無 database call。
- import 無 credentials requirement。
- tasks 有 timeout。
- retries bounded。
- quality gate 失敗會阻止成功 publish report。
- report 明確標示 local validation。
- unit tests 可直接呼叫 task-level functions。
- Airflow dependency 若與 Python 3.14 不相容，必須使用獨立 environment 或只驗證 pure functions；不得默默更換整個專案 Python。

## 11A.12 Required Test Scenarios

### Scenario A — Valid Partitioned Query

- query 有 `event_date` bounded predicate。
- 無 `SELECT *`。
- simulated bytes 在 threshold 內。
- result = pass。

### Scenario B — Missing Partition Filter

- query mart partition table。
- 無 date predicate。
- result = block。

### Scenario C — Exceeds Threshold

- fixture bytes 超過 threshold。
- result = block。
- report 顯示 simulated。

### Scenario D — Invalid Query Fixture

- valid = false。
- result = invalid。
- 不產生 bytes = 0。

### Scenario E — Unknown SQL Parse

- parser 無法解析。
- result = unknown。
- 需要 manual review。

### Scenario F — Cross-currency Aggregation

- amount aggregation 缺少 currency grouping。
- result = block 或 warning，依 published policy。

### Scenario G — DAG Import Without Credentials

- 沒有 GCP credentials。
- DAG import 仍成功。
- cloud tasks 不存在或明確 disabled。

### Scenario H — Quality Gate Failure

- 任一 blocking policy 失敗。
- publish task 不得報告 overall success。

## 11A.13 Required Commands

命名可依 Makefile conventions 微調：

```bash
make bigquery-static-validate
make bigquery-partition-policy
make bigquery-cost-policy
make bigquery-cost-report
make airflow-parse
make test-airflow
make test-bigquery-policy
```

所有命令：

- 不需要 GCP credentials。
- 不需要 Billing。
- 不建立 cloud resources。
- 非零 exit code 表示 blocking failure。
- report 必須標示 simulated 或 static_validation。

## 11A.14 Acceptance Criteria

- [ ] PostgreSQL dbt baseline 仍可執行。
- [ ] BigQuery compatibility metadata 有 schema。
- [ ] Required marts 有 partition / clustering policy。
- [ ] Partition field 存在於 contract。
- [ ] `require_partition_filter` policy 可驗證。
- [ ] `SELECT *` 可偵測。
- [ ] Missing partition predicate 可阻擋。
- [ ] Cross-currency aggregation risk 可偵測。
- [ ] Cost threshold config 有單一來源。
- [ ] Dry-run fixture parser 有 schema validation。
- [ ] Simulated report 明確標示 simulated。
- [ ] Report 沒有 fake job ID。
- [ ] Airflow DAG 可在無 credentials 時 import。
- [ ] Airflow retry / timeout 有測試。
- [ ] Quality gate 可阻止成功狀態。
- [ ] Phase 1～7 tests 仍通過。
- [ ] README 不宣稱真實 BigQuery pipeline 已完成。

## 11A.15 Completion Gate

```bash
make data-platform-fixtures
make dbt-build
make dbt-test
make bigquery-static-validate
make bigquery-partition-policy
make bigquery-cost-policy
make bigquery-cost-report
make airflow-parse
make test-airflow
make test-bigquery-policy
make lint
make typecheck
make test
make test-data-platform
```

Phase 8A 完成後：

- 可以開始 Phase 9。
- 不需要先完成 Phase 8B。
- 不需要先完成 Phase 8C。
- 不得自動開始 Phase 8B、8C 或 9。

---

# 11B. Phase 8B — BigQuery Sandbox Validation

## 11B.1 Requirement

```text
optional
```

Phase 8B 不需要信用卡或 Billing Account，但依賴使用者自行建立 BigQuery Sandbox project。

Codex 不得：

- 自動替使用者建立 Google Cloud account。
- 自動接受服務條款。
- 要求使用者提供信用卡。
- 在無明確要求下建立或刪除 project。
- 宣稱 Sandbox 可測試不支援的 DML/streaming。

## 11B.2 目標

使用免費 BigQuery Sandbox，取得真實但受限的 BigQuery evidence：

- BigQuery SQL dialect validation
- query validator result
- estimated processed bytes shown by BigQuery
- actual query job information
- bounded public-dataset query
- partition-filter concept demonstration
- repository evidence record

## 11B.3 Scope

Phase 8B 包含：

- 手動建立 Sandbox project
- 記錄 project ID 的 non-secret alias
- 查詢 BigQuery public dataset
- 執行 repository 內的 Sandbox-safe SQL
- 保存 query text
- 保存 job ID 或 job information
- 保存 bytes processed
- 保存執行日期
- 保存 Sandbox limitation note
- optional small CSV load through supported Sandbox flow
- optional view / table experiment only when official Sandbox capability permits

## 11B.4 Non-goals

Phase 8B 不包含：

- Billing
- DML
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- streaming
- Data Transfer Service
- dbt incremental
- PostgreSQL-to-BigQuery sync
- Cloud Composer
- production dataset
- service-account automation
- cost guarantee
- permanent table retention
- full Phase 8C acceptance

## 11B.5 Required Sandbox Query

Repository 應提供至少一個 Sandbox-safe query，例如：

```text
docs/data-platform/sandbox_queries/
├── public_dataset_partition_demo.sql
├── public_dataset_select_star_bad.sql
└── README.md
```

Query requirements：

- 使用官方 public dataset。
- 有 bounded date / partition predicate，若 dataset 支援。
- 不使用 DML。
- 不要求建立 permanent table。
- 不使用未公開 credentials。
- 預期 processed bytes 有合理上限。
- 執行前檢查 BigQuery UI 顯示的 estimated bytes。
- 若 estimated bytes 超出自訂 Sandbox threshold，停止執行。

## 11B.6 Evidence Record

產出：

```text
reports/cost/bigquery-sandbox-evidence.json
reports/cost/bigquery-sandbox-evidence.md
```

JSON：

```json
{
  "report_type": "bigquery_sandbox_validation",
  "schema_version": 1,
  "evidence_level": "sandbox_observed",
  "provider": "bigquery_sandbox",
  "billing_enabled": false,
  "sandbox_constraints_verified_at": "ISO-8601 timestamp",
  "query_name": "public_dataset_partition_demo",
  "query_sha256": "",
  "job_id": "",
  "location": "",
  "total_bytes_processed": 0,
  "cache_hit": false,
  "status": "success",
  "limitations": [
    "No DML validation",
    "No streaming validation",
    "No incremental MERGE validation"
  ],
  "generated_at": "ISO-8601 timestamp"
}
```

注意：

- 真實結果為 0 時才可填 0。
- 無法取得時使用 null。
- 不提交 project number、credential 或 account email。
- project ID 可遮罩，例如 `portfolio-sandbox-***`。
- screenshot 可以作為補充，不得作為唯一 machine-readable evidence。

## 11B.7 Required Demonstrations

### Demo A — Query Size Awareness

- 在執行前查看 estimated bytes。
- 保存 estimate 或 UI evidence。
- query 有明確 limit / partition predicate。
- 不使用 `SELECT *`。

### Demo B — Bad Query Policy

- repository 保留一個不執行的 bad query。
- local policy validator 必須阻擋。
- 不需要真的花 Sandbox quota 執行 bad query。

### Demo C — Sandbox Limitation

文件明確說明：

```text
Sandbox validation does not prove DML, streaming,
incremental MERGE, or production orchestration.
```

## 11B.8 Acceptance Criteria

- [ ] Official Sandbox limits 在執行前重新確認。
- [ ] No Billing Account。
- [ ] No credit card requirement。
- [ ] 至少一個真實 Sandbox query 成功。
- [ ] 真實 job evidence 已保存。
- [ ] 真實 bytes processed 已保存或清楚標示 unavailable。
- [ ] Query 使用 bounded scan。
- [ ] Report evidence level = `sandbox_observed`。
- [ ] Report 沒有 credentials。
- [ ] README 清楚寫出 Sandbox limitations。
- [ ] 不宣稱 DML、streaming 或 incremental 已驗證。
- [ ] Phase 8A tests 仍通過。

## 11B.9 Completion Gate

Phase 8B 主要包含手動 Google Cloud 操作，因此 completion report 必須包含：

```text
Official constraints verification date
Sandbox project alias
Query file
Query hash
Job ID
Location
Bytes processed
Cache hit
Execution status
Limitations
```

Local commands：

```bash
make sandbox-evidence-validate
make bigquery-cost-report
make test-bigquery-policy
```

Phase 8B 完成後，不得自動開始 Phase 8C。

---

# 11C. Phase 8C — Full BigQuery Pipeline

## 11C.1 Requirement

```text
optional_deferred
```

只有使用者明確決定啟用 Billing 後才可開始。

尚未啟用 Billing 時：

- 狀態應為 `deferred`。
- 不得標示 `blocked`。
- 不影響 Phase 9、10。
- 不影響本機作品集完成。
- README 必須標示 planned / optional。

## 11C.2 Start Preconditions

開始前必須全部符合：

- 使用者明確要求開始 Phase 8C。
- 使用者知道 Billing-enabled project 可能產生費用。
- project 與 region 已確認。
- budget / alert strategy 已定義。
- credentials strategy 已定義。
- resource cleanup plan 已定義。
- maximum bytes policy 已定義。
- estimated demo cost 已書面說明。
- 使用者明確核准。

Codex 不得自行：

- 啟用 Billing。
- 建立付費 resource。
- 提升 IAM。
- 修改 budget。
- 執行未經核准的大型 query。

## 11C.3 目標

建立真實但 bounded 的 BigQuery data pipeline：

```text
PostgreSQL sources
→ bounded batch extraction
→ BigQuery raw datasets
→ dbt-bigquery
→ incremental marts
→ quality gate
→ metadata artifacts
```

並驗證：

- idempotent batch load
- watermark
- MERGE
- partition
- clustering
- late-arriving data
- query dry-run
- maximum bytes guardrail
- Airflow orchestration
- real cloud evidence

## 11C.4 Scope

Phase 8C 包含：

- `dbt-bigquery`
- `google-cloud-bigquery`
- BigQuery profiles
- dev / CI dataset isolation
- bounded PostgreSQL extraction
- load jobs
- idempotent MERGE
- watermark persistence
- Airflow DAG
- incremental dbt mart
- partition and clustering
- query dry-run
- maximum bytes billed or equivalent guardrail
- quality gate
- cloud run report
- cleanup command
- real job IDs
- real bytes processed
- cost observation

## 11C.5 Non-goals

Phase 8C 仍不包含：

- production SLA
- 24/7 scheduler
- Terraform platform
- production Cloud Composer deployment
- Dataflow
- Pub/Sub migration
- Dataplex deployment
- enterprise IAM automation
- production secrets platform
- high-volume benchmark claim
- autonomous cost optimization
- autonomous remediation

## 11C.6 Dependency Isolation

建議 dependency groups：

```toml
[dependency-groups]
data-platform = [
  # dbt-core
  # dbt-postgres
]

gcp = [
  # dbt-bigquery
  # google-cloud-bigquery
  # Airflow provider dependencies
]

mcp = [
  # selected in Phase 9
]
```

版本規則：

- 實作時實際驗證 Python 3.14 compatibility。
- 不得在規格中預先捏造相容版本。
- Airflow 若不相容，使用獨立 environment。
- 不得默默降低主專案 Python 版本。
- version lock 必須附測試 evidence。

## 11C.7 Dataset Layout

建議：

```text
<project>.retail_raw_dev
<project>.retail_analytics_dev_<developer>
<project>.retail_analytics_ci_<run_id>
```

規則：

- production dataset 不在此 Phase 建立。
- dev / CI dataset 不共用寫入。
- dataset location 一致。
- CI dataset 設定 expiration。
- labels 標示 environment 與 owner。
- credentials 不提交。
- least privilege。
- cleanup 有明確 command。

## 11C.8 Raw BigQuery Tables

### `valid_orders`

Partition：

```text
DATE(event_time)
```

Cluster：

```text
order_id, event_type
```

Merge key：

```text
event_id
```

### `processed_events`

Partition：

```text
DATE(processed_at)
```

Cluster：

```text
consumer_group, topic
```

Merge key：

```text
consumer_group, event_id
```

### `log_metrics_minute`

Partition：

```text
DATE(metric_minute)
```

Cluster：

```text
service, endpoint
```

Merge key：

```text
metric_minute, service, endpoint
```

## 11C.9 PostgreSQL-to-BigQuery Sync

Sync 必須：

- bounded batch
- deterministic ordering
- watermark
- tie-breaker
- idempotent load
- retry classification
- no source deletion
- partial failure report
- no watermark advance on failed commit
- row counts and checksums where practical
- explicit source / target timestamps

Watermark：

| Source | Watermark |
|---|---|
| `valid_orders` | `created_at` + `event_id` |
| `processed_events` | `processed_at` + composite key |
| `log_metrics_minute` | `updated_at` + composite key |

`log_metrics_minute` 必須 upsert，因相同 grain row 可能更新。

## 11C.10 Airflow DAG

建議 DAG ID：

```text
retail_data_platform_bigquery_pipeline
```

Logical flow：

```text
check_connections
→ capture_source_watermarks
→ extract_postgres_batches
→ stage_bigquery_load
→ merge_bigquery_raw
→ validate_raw_counts
→ dbt_build_staging
→ dbt_build_marts
→ quality_gate
→ publish_artifacts
→ write_cloud_run_report
```

Requirements：

- DAG import 無 side effects。
- task timeout。
- bounded retries。
- retry policy 有文件。
- logs 無 credentials。
- backfill range bounded。
- idempotent tasks。
- quality failure 不 publish success。
- report 有 cloud evidence。
- no automatic offset reset。

## 11C.11 Incremental Model

至少一個 BigQuery incremental mart，建議：

```text
mart_daily_sales
```

Requirements：

- unique key 明確。
- partition config 明確。
- bounded lookback window。
- late-arriving event test。
- same-date rerun safe。
- full refresh equivalence test。
- partition predicate。
- no `max(date)` only strategy。
- compiled SQL review。
- real execution evidence。

## 11C.12 Cost Governance

每個真實 BigQuery query validation：

- dry-run
- estimated bytes
- threshold
- decision
- job ID
- project alias
- location
- timestamp

政策：

- mart query 必須帶 partition filter。
- 禁止不必要 `SELECT *`。
- join explosion warning。
- bytes 超過 threshold 預設 block。
- override 需要理由與 audit。
- demo query 使用 `maximum_bytes_billed` 或等價 guardrail，若 API 支援。
- query 完成後記錄實際 bytes。
- 不將 free-tier status 當作永遠免費保證。

## 11C.13 Local / Cloud Separation

Local mandatory：

- DAG import
- task unit tests
- config validation
- watermark logic
- merge-key logic
- cost policy
- report parsing
- Phase 1～10 non-cloud tests

Cloud acceptance：

- datasets 建立
- fixture load
- MERGE rerun
- dbt build
- incremental replay
- late data replay
- partition metadata check
- clustering metadata check
- dry-run evidence
- actual job IDs
- cleanup validation

## 11C.14 Cloud Report

產出：

```text
reports/cost/bigquery-cloud-evidence.json
reports/cost/bigquery-cloud-evidence.md
```

必須：

```json
{
  "report_type": "bigquery_cloud_validation",
  "schema_version": 1,
  "evidence_level": "cloud_observed",
  "provider": "bigquery",
  "billing_enabled": true,
  "project_alias": "",
  "location": "",
  "jobs": [],
  "estimated_bytes": 0,
  "actual_bytes_processed": 0,
  "cost_observation": {
    "currency": null,
    "amount": null,
    "note": "Use null when exact cost is not available."
  },
  "generated_at": ""
}
```

不得因使用 free tier 而將 cost 永久寫成 0。

## 11C.15 Acceptance Criteria

- [ ] 使用者明確核准 Billing-enabled execution。
- [ ] Budget / cleanup strategy 文件完成。
- [ ] Credentials 未提交。
- [ ] Dataset isolation。
- [ ] Raw tables partition / cluster 正確。
- [ ] Sync 可安全重跑。
- [ ] MERGE idempotent。
- [ ] Watermark failure behavior 正確。
- [ ] Airflow DAG 可執行。
- [ ] Incremental mart 有 late-data test。
- [ ] Dry-run 有真實 evidence。
- [ ] Cost threshold 可阻擋。
- [ ] Quality gate 可阻止 publish success。
- [ ] Cloud report evidence level = `cloud_observed`。
- [ ] Job IDs 真實。
- [ ] Cleanup command 已驗證。
- [ ] PostgreSQL local baseline 未破壞。
- [ ] Phase 1～10 non-cloud tests 仍通過。

## 11C.16 Completion Gate

Local：

```bash
make airflow-parse
make test-airflow
make test-bigquery-policy
make test-bigquery-sync
make lint
make typecheck
make test
make test-data-platform
```

Cloud：

```bash
make gcp-validate
make bigquery-create-dev-datasets
make bigquery-load-fixtures
make bigquery-sync
make dbt-build-bigquery
make bigquery-dry-run
make bigquery-incremental-replay
make bigquery-late-data-replay
make bigquery-cloud-report
make bigquery-cleanup-dry-run
```

Phase 8C 只有在 local 與 cloud gate 都通過時才可標示 `accepted`。

---

## 11.4 Phase Dependencies

```text
Phase 6 accepted
  ↓
Phase 7 accepted
  ↓
Phase 8A accepted
  ├──→ Phase 9
  │      ↓
  │    Phase 10
  │
  ├──→ Phase 8B optional
  │
  └──→ Phase 8C optional_deferred
```

Rules：

- Phase 8B 不需要 Phase 9。
- Phase 8C 建議在 Phase 8A 後執行。
- Phase 9 不依賴 8B 或 8C。
- Phase 10 不依賴 8B 或 8C。
- 8C 未完成時，BigQuery cloud tools 必須回傳 `not_available`。
- 8B 完成但 8C 未完成時，只能回傳 Sandbox evidence，不得回傳 cloud pipeline evidence。

## 11.5 Phase 8 Overall Status Rules

Phase 8 不使用單一模糊狀態。

必須分別記錄：

```yaml
phase_8a: accepted
phase_8b: optional
phase_8c: deferred
```

不得使用：

```yaml
phase_8: accepted
```

除非同時附上三個 subphase 狀態。

README 建議顯示：

```text
Phase 8A — Local compatibility and cost policy: Accepted
Phase 8B — BigQuery Sandbox validation: Optional / Not started
Phase 8C — Full BigQuery pipeline: Deferred
```

---
# 12. Phase 9 — Metadata Index and Data Discovery MCP Server

## 12.1 目標

將 dbt artifacts、quality reports、Airflow reports、Kafka lag reports 與 benchmark evidence 建立成唯讀 metadata layer，並透過 MCP tools 讓 Codex 查詢：

- data assets
- schema
- owner
- lineage
- quality
- failures
- consumer lag
- cost evidence

## 12.1.1 Start Preconditions

Phase 9 可以在以下條件成立後開始：

- Phase 6 accepted。
- Phase 7 accepted。
- Phase 8A accepted。

Phase 9 不需要：

- Phase 8B accepted。
- Phase 8C accepted。
- GCP credentials。
- Billing Account。
- BigQuery dataset。
- Cloud Composer。

若 Phase 8B 或 8C 尚未完成：

- Sandbox evidence tool 回傳 `not_available` 或不存在。
- Cloud BigQuery evidence tool 回傳 `not_available`。
- Airflow cloud run tool 回傳 `not_available`。
- 不得因 optional artifact 缺少而使整個 MCP Server 無法啟動。
- PostgreSQL dbt、quality、contract、Kafka lag 與 benchmark metadata 必須仍可使用。

## 12.2 Scope

Phase 9 包含：

- artifact readers
- normalized metadata model
- metadata index
- lineage graph
- report adapters
- MCP Server
- read-only tools
- schema validation
- timeout
- error handling
- secret redaction
- audit log
- local MCP smoke test
- Codex MCP setup documentation

## 12.3 Non-goals

Phase 9 不包含：

- unrestricted SQL
- raw row sampling
- source data export
- shell execution
- file write tool
- schema mutation
- pipeline rerun
- Kafka offset reset
- PR modification
- Agent reasoning
- automated incident remediation
- public internet MCP deployment

## 12.4 Metadata Inputs

Mandatory：

- `dbt/target/manifest.json`
- `dbt/target/catalog.json`
- `dbt/target/run_results.json`
- source freshness result
- data-quality report
- contract diff report
- Phase 7 CI summary
- Phase 8A static-validation / simulated cost report
- Kafka consumer lag JSON
- benchmark report JSON

Optional cloud evidence：

- Phase 8B BigQuery Sandbox evidence
- Phase 8C BigQuery cloud evidence
- Phase 8C Airflow cloud run report

Optional metadata：

- compiled SQL
- exposures
- semantic metadata
- GCP job metadata

所有 input：

- 必須 allowlist path。
- 必須驗證 JSON structure。
- 缺失時回傳 degraded status。
- 不得因單一 optional artifact 缺失而 crash 整個 server。

## 12.5 Normalized Asset Model

每個 asset 至少包含：

```json
{
  "asset_id": "model.retail_data_platform.fct_orders",
  "name": "fct_orders",
  "resource_type": "model",
  "layer": "marts",
  "owner": "data-platform",
  "domain": "commerce",
  "description": "Grain: one row per order_id.",
  "maturity": "experimental",
  "columns": [],
  "upstream": [],
  "downstream": [],
  "quality_status": "pass",
  "last_run_at": "ISO-8601 timestamp",
  "source_artifacts": []
}
```

不得包含：

- database password
- access token
- service-account private key
- arbitrary row values
- stack trace with secrets
- complete environment dump

## 12.6 Metadata Index

Index 必須支援：

- exact model lookup
- partial name search
- description keyword search
- column name search
- owner filter
- domain filter
- resource type filter
- upstream traversal
- downstream traversal
- quality status lookup

Index build：

```bash
make metadata-index
```

產出：

```text
reports/metadata/metadata-index.json
reports/metadata/lineage-graph.json
reports/metadata/index-summary.json
```

Index build 必須 deterministic：

- 相同 artifacts 產生相同 logical output。
- 排序穩定。
- timestamps 只放在明確 metadata 欄位。
- 不將 filesystem absolute path 暴露給 MCP consumer。

## 12.7 MCP Tools

### `search_data_assets`

Input：

```json
{
  "query": "orders",
  "resource_types": ["model", "source"],
  "owner": null,
  "domain": null,
  "limit": 20
}
```

Output：

- matched assets
- match reason
- owner
- description
- quality summary
- evidence artifact IDs

限制：

- maximum limit。
- 不接受 SQL。
- 不讀取 source rows。

### `get_model_schema`

Input：

```json
{
  "model_name": "fct_orders"
}
```

Output：

- model identity
- grain
- materialization
- columns
- data types
- nullable expectations
- descriptions
- contract status
- tests
- evidence

### `get_model_owner`

Input：

```json
{
  "model_name": "mart_daily_sales"
}
```

Output：

- owner
- domain
- data product
- maturity
- SLO
- source metadata location

### `get_lineage`

Input：

```json
{
  "model_name": "mart_daily_sales",
  "direction": "both",
  "max_depth": 3
}
```

Output：

- upstream nodes
- downstream nodes
- edges
- truncated flag
- evidence

限制：

- bounded depth。
- bounded nodes。
- cycle protection。

### `get_upstream_lineage`

`get_lineage(direction="upstream")` 的 convenience tool 或等價 interface。

### `get_downstream_impact`

Input：

```json
{
  "model_name": "stg_order_events",
  "max_depth": 5
}
```

Output：

- impacted assets
- published marts
- exposures，若有
- quality / maturity
- impact path
- truncated flag

### `get_quality_status`

Input：

```json
{
  "model_name": "fct_orders",
  "include_history": false
}
```

Output：

- overall status
- tests passed / failed / skipped
- freshness
- contract status
- last run
- failure evidence
- stale artifact warning

### `get_recent_pipeline_failures`

Input：

```json
{
  "pipeline_name": "retail_data_platform_pipeline",
  "limit": 10
}
```

Output：

- run ID
- task
- status
- started / ended
- sanitized error category
- artifact references

若 Phase 8C 未完成：

- 可以回傳 Phase 8A local validation DAG failure，若 artifact 存在。
- Cloud pipeline result 回傳 `not_available`。
- 清楚說明缺少 cloud Airflow artifact。
- 不得捏造空成功結果。

### `get_consumer_lag`

Input：

```json
{
  "consumer_group": "order-processing-group-v1"
}
```

Output：

- topic
- partition
- committed offset
- log-end offset
- lag
- observation timestamp
- status
- report evidence

限制：

- 只讀 machine-readable report 或 allowlisted live interface。
- 不提供 offset reset。

### `get_cost_estimate`

Input：

```json
{
  "model_name": "mart_daily_sales",
  "preferred_evidence_level": "best_available"
}
```

允許的 `preferred_evidence_level`：

```text
best_available
simulated
sandbox_observed
cloud_observed
```

Output 必須區分：

- Phase 8A fixture-based simulated estimate
- Phase 8B Sandbox-observed bytes
- Phase 8C cloud-observed dry-run / execution bytes
- unavailable

Example：

```json
{
  "model_name": "mart_daily_sales",
  "evidence_level": "simulated",
  "estimated_bytes": 104857600,
  "threshold_bytes": 536870912,
  "decision": "pass",
  "observed_job_id": null,
  "warning": "Fixture-based estimate; not a real BigQuery measurement."
}
```

Rules：

- 不得將 simulated 結果升級為 cloud evidence。
- 只有 Phase 8B 可回傳 `sandbox_observed`。
- 只有 Phase 8C 可回傳 `cloud_observed`。
- optional artifact 缺失時回傳 `not_available`。
- missing bytes 不得填 0。

## 12.8 Tool Response Standard

每個 tool response 至少包含：

```json
{
  "status": "ok",
  "data": {},
  "evidence": [],
  "evidence_level": "static_validation",
  "warnings": [],
  "generated_at": "ISO-8601 timestamp"
}
```

允許 status：

```text
ok
partial
not_found
not_available
invalid_request
error
```


允許 `evidence_level`：

```text
static_validation
simulated
sandbox_observed
cloud_observed
not_available
```

規則：

- `partial` 必須解釋缺少什麼。
- `not_available` 不等於 empty result。
- error 不得暴露 secret。
- evidence 必須能追溯到 artifact 類型或 report ID。
- 不得要求 Agent 只相信自然語言 summary。

## 12.9 Security

MCP Server：

- 預設 bind localhost。
- 不提供 public network listener。
- 所有 file access 使用 allowlist。
- 拒絕 `..` path traversal。
- 不接受任意 filesystem path。
- 不接受任意 URL fetch。
- 不執行 shell。
- 不執行 SQL。
- 不回傳 raw environment variables。
- 設定 request timeout。
- 設定 result size limit。
- 對 log 中疑似 secret 做 redaction。
- audit log 不記錄 credentials。

## 12.10 Audit Log

每次 tool call 記錄：

- request ID
- timestamp
- tool name
- sanitized arguments
- status
- duration
- result size
- evidence count
- error category，若有

不得記錄：

- token
- password
- private key
- raw connection string
- unrestricted payload

Audit log 建議：

```text
reports/metadata/mcp-audit.jsonl
```

## 12.11 MCP Test Scenarios

- 搜尋 `orders` 找到相關 assets。
- 查 `fct_orders` schema。
- 查 `stg_order_events` downstream impact。
- 查不存在 model 回傳 `not_found`。
- 缺少 catalog 時回傳 `partial`。
- malformed manifest 被拒絕。
- path traversal 被拒絕。
- request timeout 有清楚 error。
- consumer lag report 缺失回傳 `not_available`。
- response 不含 secret。
- audit log 正確產生。

## 12.12 Required Commands

```bash
make metadata-index
make metadata-validate
make mcp-server
make mcp-smoke
make test-mcp
```

## 12.13 Phase 9 Acceptance Criteria

- [ ] Metadata readers 可解析 required artifacts。
- [ ] Metadata index deterministic。
- [ ] Lineage graph 可查 upstream / downstream。
- [ ] Required MCP tools 全部存在。
- [ ] Tools 預設唯讀。
- [ ] No arbitrary SQL。
- [ ] No shell execution。
- [ ] No arbitrary filesystem path。
- [ ] Input / output schemas 有驗證。
- [ ] Timeout 有測試。
- [ ] Secret redaction 有測試。
- [ ] Audit log 有測試。
- [ ] Missing optional cloud artifact 回傳 `not_available` 或 degraded status。
- [ ] Simulated、Sandbox 與 Cloud evidence 不會混淆。
- [ ] Phase 8B / 8C 未完成時 MCP 仍可啟動。
- [ ] Codex 可透過文件完成本機 MCP 設定。
- [ ] Phase 1～8 tests 仍通過。

## 12.14 Phase 9 Completion Gate

```bash
make metadata-index
make metadata-validate
make mcp-smoke
make test-mcp
make lint
make typecheck
make test
make test-data-platform
```

完成後停止，不得自動開始 Phase 10。

---

# 13. Phase 10 — Codex Skills and Incident Diagnosis Agent

## 13.1 目標

以 Codex 作為主要 AI coding agent，透過 repository-local Skills 與 Phase 9 MCP tools，建立三個可重複、受限制且可驗證的 workflow：

1. dbt model scaffold
2. dbt PR review
3. data incident diagnosis

本 Phase 不要求使用 Claude Code。

## 13.2 Scope

Phase 10 包含：

- `.agents/skills/`
- Skill documentation
- deterministic helper scripts
- templates
- validation steps
- sample prompts
- MCP-backed context retrieval
- incident workflow
- evidence-based result format
- confidence / uncertainty
- demo transcripts or reports
- test fixtures
- human approval boundary

## 13.3 Non-goals

Phase 10 不包含：

- autonomous code merge
- autonomous production deployment
- autonomous schema mutation
- autonomous pipeline rerun
- autonomous Kafka offset reset
- unrestricted SQL
- arbitrary shell
- multi-agent swarm
- long-term memory service
- custom LLM training
- mandatory paid API integration
- production incident management platform

## 13.4 Skill Structure

每個 Skill：

```text
.agents/skills/<skill-name>/
├── SKILL.md
├── scripts/       # optional
├── templates/     # optional
├── references/    # optional
└── fixtures/      # optional
```

每個 `SKILL.md` 必須包含：

- name
- description
- invocation conditions
- required inputs
- required context
- allowed tools
- prohibited actions
- execution steps
- validation steps
- expected output
- failure handling
- completion criteria

Skill 不得 override：

- explicit user instruction
- `AGENTS.md`
- `SPEC.md`
- `DATA_PLATFORM_SPEC.md`
- security boundaries

## 13.5 Skill 1 — `dbt-scaffold`

建議路徑：

```text
.agents/skills/dbt-scaffold/
├── SKILL.md
├── scripts/
│   ├── inspect_available_columns.py
│   ├── scaffold_model.py
│   └── validate_generated_model.py
├── templates/
│   ├── staging.sql.j2
│   ├── intermediate.sql.j2
│   ├── mart.sql.j2
│   ├── model.yml.j2
│   └── unit_test.yml.j2
└── references/
    ├── modeling-conventions.md
    ├── quality-requirements.md
    └── contract-policy.md
```

### Required Workflow

1. 閱讀 `AGENTS.md`。
2. 閱讀本文件當前 Phase。
3. 確認使用者需求與 model grain。
4. 透過 MCP 或 repository inspection 查詢可用 sources/models/columns。
5. 若欄位不存在，停止並回報。
6. 決定 layer。
7. 列出預計建立或修改檔案。
8. 產生 SQL。
9. 產生 YAML。
10. 產生 contract。
11. 產生 tests。
12. 執行 convention validation。
13. 執行 `dbt parse`。
14. 執行 `dbt compile`。
15. 執行 affected `dbt build`。
16. 回報 command、exit code、結果與未解決問題。
17. 不自動 commit。

### Required Inputs

- business requirement
- grain
- expected consumers
- owner
- domain
- required metrics
- acceptable freshness

若缺少 minor input：

- 可使用清楚 placeholder。
- 不得猜測 source schema。
- 不得猜測 financial semantics。

### Prohibited

- 猜測不存在欄位
- 建立假的 source
- 跳過 failed tests
- 自動修改 production schema
- 自動執行 production pipeline
- 將 compile success 等同 business correctness
- 將未驗證 code 描述為完成

### Output

至少包含：

- requirement summary
- selected sources
- selected layer
- grain
- generated files
- validation commands
- validation results
- warnings
- assumptions
- remaining human-review items

## 13.6 Skill 2 — `dbt-pr-review`

建議路徑：

```text
.agents/skills/dbt-pr-review/
├── SKILL.md
├── scripts/
│   ├── detect_changed_models.py
│   ├── compare_contracts.py
│   ├── validate_model_conventions.py
│   └── summarize_lineage_impact.py
└── references/
    ├── review-checklist.md
    ├── sql-antipatterns.md
    ├── breaking-change-policy.md
    └── cost-policy.md
```

### Review Scope

必須檢查：

- model layer responsibility
- grain clarity
- direct source usage
- inappropriate `select *`
- missing model description
- missing column description
- missing owner
- missing SLO
- missing contract
- missing required tests
- breaking column removal
- breaking type change
- grain change
- unsafe incremental logic
- missing lookback for late data
- missing partition filter
- possible BigQuery full scan
- join explosion risk
- duplicated business logic
- money / currency handling
- divide-by-zero
- weighted average correctness
- downstream impact
- generated docs impact
- test coverage

### Finding Levels

```text
blocking
warning
suggestion
```

每個 finding：

```json
{
  "severity": "blocking",
  "file": "dbt/models/marts/fct_orders.sql",
  "model": "fct_orders",
  "rule": "published-contract-breaking-change",
  "reason": "order_id was removed",
  "impact": "downstream marts cannot preserve order grain",
  "recommendation": "restore the column or version the data product",
  "evidence": []
}
```

### Prohibited

Review Skill 不得：

- approve PR
- merge PR
- push code
- modify production resource
- silently fix findings
- suppress blocking findings without reason

## 13.7 Skill 3 — `incident-diagnosis`

建議路徑：

```text
.agents/skills/incident-diagnosis/
├── SKILL.md
├── references/
│   ├── incident-workflow.md
│   ├── evidence-policy.md
│   └── remediation-boundaries.md
└── fixtures/
    ├── freshness_failure.json
    ├── quality_failure.json
    └── lag_failure.json
```

Codex 使用此 Skill，並透過 Phase 9 MCP tools 取得證據。

## 13.8 Incident Input

標準 alert：

```json
{
  "incident_id": "INC-20260806-001",
  "alert_type": "freshness",
  "asset": "mart_daily_sales",
  "observed_at": "2026-08-06T02:00:00Z",
  "severity": "high",
  "message": "Data freshness exceeded the configured SLO."
}
```

Required fields：

- incident ID
- alert type
- asset or pipeline
- observed time
- severity
- message

## 13.9 Diagnosis State Machine

```text
RECEIVED
→ VALIDATED
→ ASSET_RESOLVED
→ QUALITY_CHECKED
→ LINEAGE_TRACED
→ PIPELINE_CHECKED
→ KAFKA_CHECKED
→ EVIDENCE_CORRELATED
→ DIAGNOSIS_PRODUCED
→ HUMAN_REVIEW_REQUIRED
```

任一工具失敗：

```text
DEGRADED_DIAGNOSIS
```

不得因工具失敗直接捏造 root cause。

## 13.10 Required Investigation Steps

1. 驗證 alert schema。
2. 解析 asset。
3. 查 owner、SLO 與 maturity。
4. 查最新 quality status。
5. 查 source freshness。
6. 查 upstream lineage。
7. 查 downstream impact。
8. 查最近 local validation 或 cloud pipeline failures，若 artifact 可用。
9. 查 Kafka consumer lag。
10. 查 BigQuery cost / job evidence，並辨識 simulated、Sandbox 或 Cloud level。
11. 若 Phase 8B / 8C 未完成，將 cloud evidence 標示為 unavailable，不得視為 failure。
12. 查 allowlisted sanitized logs，若有。
13. 建立 timeline。
14. 區分 facts、inferences、unknowns。
15. 產生 possible root cause。
16. 列出 affected assets。
17. 提出 remediation plan。
18. 提出 backfill validation plan。
19. 標示需要人工核准的動作。

## 13.11 Evidence Policy

Diagnosis 必須將內容分為：

### Confirmed Facts

由 tool evidence 直接支持。

### Inferences

根據多個 facts 推論，必須附 confidence：

```text
high
medium
low
```

### Unknowns

目前 artifacts 無法回答的問題。

### Rejected Hypotheses

經證據排除的可能性。

不得：

- 把 inference 寫成 fact。
- 省略 evidence ID。
- 因 event coincidence 宣稱 causation。
- 在沒有 log 或 failure evidence 時指定單一 root cause。

## 13.12 Incident Output

Markdown 與 JSON 兩種格式。

JSON 最少包含：

```json
{
  "incident_id": "INC-20260806-001",
  "status": "degraded",
  "summary": "",
  "confirmed_facts": [],
  "hypotheses": [],
  "most_likely_cause": {
    "description": "",
    "confidence": "medium",
    "evidence": []
  },
  "affected_assets": [],
  "customer_or_business_impact": [],
  "recommended_actions": [],
  "backfill_plan": [],
  "validation_plan": [],
  "unknowns": [],
  "prohibited_actions_not_executed": [],
  "generated_at": ""
}
```

Report paths：

```text
reports/incidents/<incident_id>.json
reports/incidents/<incident_id>.md
```

## 13.13 Agent Safety Boundary

第一版只允許：

- query
- inspect
- correlate
- analyze
- summarize
- recommend
- generate a remediation plan
- generate a backfill plan
- generate validation commands for human review

第一版不得直接：

- delete tables
- truncate tables
- mutate schemas
- execute unrestricted SQL
- execute arbitrary shell commands
- rerun production pipelines
- reset Kafka offsets
- alter consumer groups
- merge PRs
- push commits
- change IAM
- create cloud resources
- acknowledge production incidents on behalf of a human

## 13.14 Required Incident Scenarios

### Scenario A — Freshness Failure Caused by Pipeline Failure

Evidence：

- mart freshness failed
- upstream model stale
- Airflow task failed
- Kafka lag may be normal

Expected：

- identify pipeline failure as supported cause
- list downstream impact
- produce rerun and backfill plan
- do not execute rerun

### Scenario B — Upstream Kafka Lag

Evidence：

- consumer lag rising
- source table freshness stale
- dbt job itself successful but processed old input

Expected：

- distinguish ingestion delay from dbt failure
- identify affected source and marts
- recommend consumer investigation
- do not reset offsets

### Scenario C — Data Quality Failure

Evidence：

- dbt uniqueness or consistency test failed
- pipeline task completed
- freshness is normal

Expected：

- identify quality gate failure
- avoid claiming ingestion outage
- provide data repair validation plan

### Scenario D — Insufficient Evidence

Evidence：

- alert exists
- Airflow artifact unavailable
- lag report stale
- quality report partial

Expected：

- output degraded diagnosis
- list unknowns
- confidence low
- no fabricated root cause

## 13.15 Optional AI API Adapter

可選擇加入 OpenAI API adapter，但：

- 不作為本 Phase 核心驗收必要條件。
- 沒有 API key 時 deterministic tools 與 Codex Skills 仍可 demo。
- API key 只能使用 environment variable。
- request / response 不得包含 credentials。
- 送入模型的 context 必須最小化。
- 不得傳送 raw PII。
- 必須保留 tool evidence。
- LLM summary 不得覆蓋 deterministic finding。

## 13.16 Skill Verification

### `dbt-scaffold`

- 正常需求可產生合規 model。
- 不存在欄位會停止。
- model 已存在會拒絕覆寫。
- 缺少 grain 的 mart 不得產生完成結果。
- 產生結果必須通過 parse / compile / affected build。

### `dbt-pr-review`

- 偵測 breaking contract。
- 偵測缺少 description。
- 偵測缺少 owner。
- 偵測不安全 incremental logic。
- 偵測 multi-currency aggregation risk。
- findings 有 severity 與 evidence。

### `incident-diagnosis`

- 正確處理 freshness failure。
- 正確沿 lineage 列出 impact。
- 正確讀取 consumer lag。
- MCP tool failure 時 degraded。
- 缺少證據時不捏造。
- 不執行任何 mutation。

## 13.17 Phase 10 Acceptance Criteria

- [ ] 三個 Codex Skills 存在。
- [ ] 每個 Skill 有完整 `SKILL.md`。
- [ ] `dbt-scaffold` 使用 metadata 查詢可用欄位。
- [ ] `dbt-scaffold` 實際執行驗證。
- [ ] `dbt-pr-review` findings 可重現。
- [ ] Contract breaking finding 為 blocking。
- [ ] Incident workflow 使用 MCP evidence。
- [ ] Incident report 區分 fact / inference / unknown。
- [ ] Tool failure 產生 degraded diagnosis。
- [ ] 第一版無 mutation tool。
- [ ] Demo report / transcript 可保存。
- [ ] No Claude Code-specific dependency。
- [ ] Phase 1～9 tests 仍通過。

## 13.18 Phase 10 Completion Gate

```bash
make skill-dbt-scaffold-smoke
make skill-dbt-pr-review-smoke
make skill-incident-diagnosis-smoke
make incident-demo
make test-skills
make test-mcp
make lint
make typecheck
make test
make test-data-platform
```

完成後不得宣稱 production autonomous agent。

---

# 14. Cross-phase Testing Strategy

## 14.1 Unit Tests

至少包含：

- metadata parsing
- lineage traversal
- contract comparison
- naming convention
- grain metadata validation
- scaffold input validation
- safe division
- weighted average
- order lifecycle logic
- watermark logic
- merge key logic
- cost threshold logic
- MCP input validation
- secret redaction
- incident evidence classification
- degraded diagnosis

## 14.2 Integration Tests

Phase 6：

- real PostgreSQL
- dbt source / build / tests
- schema isolation
- source read-only behavior

Phase 7：

- local CI simulation
- state selection
- contract diff
- deterministic fixtures

Phase 8A：

- local DAG parse
- static BigQuery SQL policy
- partition / clustering metadata validation
- fixture-based cost policy
- simulated report labeling

Phase 8B（optional）：

- real Sandbox query evidence
- bytes processed evidence
- Sandbox limitation validation

Phase 8C（optional_deferred）：

- real BigQuery batch load
- incremental rerun
- partition / clustering metadata
- real dry-run and cloud evidence

Phase 9：

- MCP server process
- real artifact files
- tool request / response
- audit log

Phase 10：

- Skill helper scripts
- MCP-backed diagnosis fixture
- report generation

## 14.3 E2E Demo

最終 demo：

```text
Start Kafka/PostgreSQL
→ Create topics and migrate
→ Generate deterministic events
→ Run consumers
→ Build dbt PostgreSQL marts
→ Show docs and quality tests
→ Introduce a controlled breaking change
→ Show CI/contract block
→ Build metadata index
→ Query schema and lineage through MCP
→ Stop a consumer or provide a controlled stale artifact
→ Run incident-diagnosis Skill
→ Produce evidence-based incident report
```

Phase 8A 必做展示：

```text
→ Validate BigQuery-compatible metadata
→ Block a query missing partition predicate
→ Parse a simulated dry-run fixture
→ Produce a report labeled simulated
```

Phase 8B 選做展示：

```text
→ Run a bounded BigQuery Sandbox query
→ Record real job information and bytes processed
→ Explain Sandbox limitations
```

Phase 8C 延後選做展示：

```text
→ Run PostgreSQL-to-BigQuery sync
→ Run incremental dbt build
→ Show real partition, dry-run and job evidence
```

## 14.4 Test Isolation

- Tests 不依賴個人 schema 名稱。
- Tests 不使用 production resources。
- Tests 使用 bounded timeout。
- Tests 清理自己建立的 CI schema / datasets。
- Tests 不重設 Kafka offsets，除非明確 isolated group。
- Tests 不刪除 Phase 1～5 benchmark evidence。
- Cloud tests 使用明確 marker。

---

# 15. Security and Privacy

## 15.1 Secrets

Secrets 只能來自：

- environment variables
- local uncommitted config
- CI secret store

不得 commit：

- `.env`
- BigQuery service-account key
- OpenAI API key
- database password
- access token
- private key
- complete connection string

## 15.2 Data Exposure

目前 `user_id` 為 identifier，應視為可能的 pseudonymous data。

MCP 與 Agent 預設：

- 不回傳 raw user rows。
- 不提供 row sampling tool。
- 不回傳 `client_ip`；目前 raw client_ip 也未落地 PostgreSQL。
- 不在 incident report 列出不必要的 user IDs。
- 只使用 aggregate、schema、metadata 與 operational evidence。

## 15.3 SQL Safety

Phase 9～10：

- 不提供任意 SQL tool。
- 若未來加入 query tool，必須另立規格。
- 必須 read-only credential。
- 必須 parser / allowlist。
- 必須 row / byte / time limits。
- 必須禁止 DDL / DML。
- 必須 audit。

## 15.4 Log Safety

- structured logs
- no secrets
- no full environment dump
- sanitize exception
- preserve error category
- keep evidence references
- bounded retention for generated audit reports

---

# 16. Documentation Requirements

每個 Phase 完成時，至少更新：

- `README.md`
- `docs/data-platform/architecture.md`
- `docs/data-platform/modeling.md`
- `docs/data-platform/quality.md`
- 對應 Phase 操作文件
- Makefile command table
- known limitations
- interview demo steps

README 必須區分：

```text
Implemented
Static validated
Simulated
Sandbox validated
Cloud validated
Planned
Optional
Deferred
Not production-ready
```

不得在 Roadmap 中使用容易讓人誤解為已完成的語氣。

BigQuery 狀態必須明確寫成其中一種：

```text
Local policy validated
Sandbox validated
Full cloud validated
Optional
Deferred
Not started
```

不得只寫 `BigQuery supported` 而未說明 evidence level。

---

# 17. Observability and Reports

所有新增流程應盡量產生 machine-readable reports。

## 17.1 Report Types

```text
reports/data-quality/
reports/cost/
reports/metadata/
reports/incidents/
```

## 17.2 Common Report Metadata

```json
{
  "report_type": "",
  "schema_version": 1,
  "run_id": "",
  "generated_at": "",
  "environment": "",
  "status": "",
  "inputs": [],
  "results": {},
  "warnings": [],
  "errors": []
}
```

## 17.3 Rules

- report schema 有版本。
- failed run 也要保存 report。
- 不虛構缺少的 metrics。
- unavailable 使用 null 或 explicit status。
- 不把 missing value 填成 0。
- 不將 stale report 當作 current evidence。
- report timestamps 使用 UTC。

---

# 18. Backward Compatibility

任何 Phase 6、7、8A、8B、8C、9、10 change 都不得默默改變：

- Kafka event models
- topic routing
- partition strategy
- consumer group
- transaction behavior
- offset commit behavior
- retry / DLQ behavior
- Phase 1～5 test semantics
- benchmark metric semantics
- report filenames already referenced by README，除非提供 migration

若 cross-track change 無法避免：

1. 提出 RFC-style summary。
2. 列出 backward compatibility impact。
3. 更新 `SPEC.md` 與本文件。
4. 更新 tests。
5. 提供 migration / rollback。
6. 使用者核准後才實作。

---

# 19. Overall Definition of Done

## 19.1 Mandatory Portfolio Mainline

本機作品集主線完成條件是：

```text
Phase 6 accepted
Phase 7 accepted
Phase 8A accepted
Phase 9 accepted
Phase 10 accepted
```

Phase 8B 與 Phase 8C 不屬於 mandatory mainline。

Mandatory mainline 必須同時符合：

- [ ] Phase 1～5 baseline tests 仍通過。
- [ ] Phase 6 dbt PostgreSQL path 可由 clean checkout 重現。
- [ ] Sources、staging、intermediate、marts 責任清楚。
- [ ] Published marts 有 grain、owner、contract、tests、SLO。
- [ ] dbt docs 可產生。
- [ ] Phase 7 scaffold 可重複使用。
- [ ] Slim CI 可選取 modified models 與 downstream。
- [ ] Breaking contract 可阻擋。
- [ ] Phase 8A BigQuery compatibility metadata 可驗證。
- [ ] Phase 8A partition-filter policy 可阻擋不合規 query。
- [ ] Phase 8A cost fixtures 與 reports 清楚標示 simulated。
- [ ] Phase 8A Airflow validation DAG 可在無 credentials 時 parse。
- [ ] Phase 8B 未完成不阻擋 Phase 9、10。
- [ ] Phase 8C 未完成不阻擋 Phase 9、10。
- [ ] Metadata index 可由 artifacts 重建。
- [ ] MCP tools 唯讀、受限制、可稽核。
- [ ] MCP 正確區分 simulated、Sandbox 與 Cloud evidence。
- [ ] Codex Skills 有清楚 workflow 與 validation。
- [ ] Incident diagnosis 使用 evidence。
- [ ] Incident report 區分 fact、inference、unknown。
- [ ] Agent 不執行 mutation。
- [ ] No secrets committed。
- [ ] README 與實作一致。
- [ ] 不宣稱 production-ready。
- [ ] 不虛構 cloud execution、成本、效能或 AI 能力。

## 19.2 Optional Phase 8B Completion

只有符合以下條件，才可宣稱 BigQuery Sandbox validated：

- [ ] 執行前重新確認官方 Sandbox restrictions。
- [ ] 真實 Sandbox query 成功。
- [ ] 真實 job evidence 保存。
- [ ] 真實 bytes processed 保存或清楚標示 unavailable。
- [ ] Report evidence level = `sandbox_observed`。
- [ ] README 說明無 DML、streaming、incremental validation。

## 19.3 Optional Phase 8C Completion

只有符合以下條件，才可宣稱 Full BigQuery Pipeline validated：

- [ ] 使用者明確核准 Billing-enabled execution。
- [ ] 真實 PostgreSQL-to-BigQuery sync 執行。
- [ ] 真實 MERGE / incremental rerun 驗證。
- [ ] 真實 partition / clustering metadata 驗證。
- [ ] 真實 dry-run evidence。
- [ ] 真實 BigQuery job IDs。
- [ ] Cloud report evidence level = `cloud_observed`。
- [ ] Cleanup 已驗證。
- [ ] Cost / billing limitation 說明完整。

## 19.4 Claim Matrix

| Evidence | 可以宣稱 | 不可以宣稱 |
|---|---|---|
| Phase 8A | BigQuery-compatible policies and local cost guardrails | BigQuery pipeline executed |
| Phase 8B | BigQuery Sandbox SQL and bytes awareness validated | DML / streaming / incremental validated |
| Phase 8C | Bounded full cloud pipeline validated | Production-ready enterprise platform |

---

# 20. Phase Status Tracking

文件只描述規格，不代表已完成。

建議 README 或獨立 status 文件使用：

| Phase | Requirement | Initial status | Evidence |
|---|---|---|---|
| Phase 1～5 | Existing baseline | 依現有驗收結果 | `SPEC.md`、tests、reports |
| Phase 6 | Mandatory | `not_started` | pending |
| Phase 7 | Mandatory | `not_started` | pending |
| Phase 8A | Mandatory | `not_started` | pending |
| Phase 8B | Optional, no Billing | `optional` | pending |
| Phase 8C | Optional, Billing required | `deferred` | pending |
| Phase 9 | Mandatory | `not_started` | pending |
| Phase 10 | Mandatory | `not_started` | pending |

允許狀態：

```text
not_started
in_progress
implementation_complete_acceptance_pending
accepted
blocked
optional
deferred
not_applicable
```

Rules：

- `deferred` 不等於失敗。
- `optional` 不等於已完成。
- 不得只因 code exists 就標示 `accepted`。
- Phase 8 必須分開記錄 8A、8B、8C。
- Phase 8B / 8C 未完成不得將 mandatory mainline 標示 blocked。
- `accepted` 必須附 evidence。

Example：

```yaml
phase_6: accepted
phase_7: accepted
phase_8a: accepted
phase_8b: optional
phase_8c: deferred
phase_9: in_progress
phase_10: not_started
```

---

# 21. Suggested Phase Prompts

## 21.1 Phase 6 Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行 Phase 6：dbt Data Products on PostgreSQL。
請先檢查目前 repository 與實際 PostgreSQL schema，摘要 Phase 6 需求，
列出預計新增或修改檔案、dependency compatibility 風險、資料模型 grain、
測試策略與 Completion Gate。

不得開始 Phase 7。
不得加入 BigQuery、Airflow、MCP 或 Agent。
不得修改 Phase 1～5 Kafka reliability semantics。

等我確認後再實作。
```

## 21.2 Phase 7 Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行 Phase 7：Paved Road and Slim CI。
先檢查 Phase 6 是否已通過驗收，再說明 scaffold interface、
convention validator、state artifacts、Slim CI selection、
contract breaking policy、GitHub Actions 流程與測試情境。

不得開始 Phase 8A。
不得加入真實 BigQuery、MCP 或 Agent。
不得要求 GCP credentials 或 Billing。

等我確認後再實作。
```

## 21.3 Phase 8A Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行 Phase 8A：
Local BigQuery Compatibility and Cost Policy。

本階段不得要求 GCP credentials、信用卡或 Billing。
不得建立任何 cloud resource。
不得將 fixture-based result 描述為真實 BigQuery measurement。

請先檢查 Phase 7 是否已通過驗收，再提出：

- BigQuery compatibility metadata
- partition / clustering policy
- require-partition-filter policy
- SELECT * detection
- partition predicate detection
- currency aggregation guard
- cost threshold config
- dry-run fixture schema
- simulated report schema
- Airflow local validation DAG
- tests and completion gate

所有 simulated reports 必須包含：
evidence_level = simulated
observed_job_id = null

不得開始 Phase 8B、8C 或 9。

等我確認後再實作。
```

## 21.4 Phase 8B Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行選做的 Phase 8B：
BigQuery Sandbox Validation。

開始前先確認並記錄 Google Cloud 官方 Sandbox restrictions。
不得要求信用卡或 Billing Account。
不得執行 DML、MERGE、streaming 或 Cloud Composer。

請提出：

- Sandbox-safe public dataset query
- expected bounded scan
- evidence capture format
- job ID and bytes processed recording
- credential and project-ID redaction
- Sandbox limitations documentation
- local evidence validator

不得將 Sandbox evidence 描述為 full cloud pipeline。
不得開始 Phase 8C。

等我確認後再執行任何需要我手動操作的步驟。
```

## 21.5 Phase 8C Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在評估選做的 Phase 8C：
Full BigQuery Pipeline。

本階段可能產生費用。
在我明確核准前，不得啟用 Billing、建立付費 resource、
執行 BigQuery query、建立 dataset 或變更 IAM。

先提出：

- estimated bounded demo scope
- possible cost exposure
- budget and cleanup plan
- project / region decisions
- dependency compatibility
- PostgreSQL-to-BigQuery sync
- watermark and MERGE keys
- Airflow DAG
- incremental and late-data tests
- dry-run and maximum-bytes guardrails
- real cloud evidence requirements
- rollback and cleanup

等我明確核准後才可實作。
```

## 21.6 Phase 9 Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行 Phase 9：
Metadata Index and Data Discovery MCP Server。

開始前確認 Phase 6、7、8A 已通過驗收。
Phase 8B 與 8C 不是前置條件。

請檢查 dbt artifacts 與現有 machine-readable reports，
說明 normalized metadata model、lineage graph、
每個 MCP tool 的 input/output schema、
security boundary、timeout、secret redaction、
audit log、degraded behavior 與 evidence level。

MCP 必須區分：

- static_validation
- simulated
- sandbox_observed
- cloud_observed
- not_available

所有 tools 必須 read-only。
不得提供 arbitrary SQL、shell、filesystem write、
pipeline rerun、schema mutation 或 offset reset。
不得開始 Phase 10。

等我確認後再實作。
```

## 21.7 Phase 10 Prompt

```text
請先閱讀 AGENTS.md、SPEC.md 與 DATA_PLATFORM_SPEC.md。

現在只執行 Phase 10：
Codex Skills and Incident Diagnosis Agent。

本專案使用 Codex，不建立 Claude Code 專屬依賴。
Phase 8B 與 Phase 8C 不是前置條件。

先說明三個 Skills 的目錄、SKILL.md workflow、
allowed tools、validation steps、incident state machine、
evidence policy、fact/inference/unknown 分類、
degraded behavior 與 mutation safety boundary。

Incident diagnosis 必須辨識：
simulated、Sandbox、Cloud 與 unavailable evidence。

不得執行 production mutation。
不得自動 commit、push、merge、rerun pipeline、
建立 cloud resource 或 reset Kafka offset。

等我確認後再實作。
```

---

# 22. Open Decisions

以下決策應在對應 Phase 開始時，依實際 dependency 與 repository 狀況確認，不得在本規格中假裝已決定：

- dbt-core / adapter 的最終相容版本
- Python 3.14 下 Airflow 的執行方式
- Airflow 是否使用獨立 virtualenv
- 是否執行 Phase 8B Sandbox validation
- Phase 8B Sandbox project alias / location
- 是否延後或開始 Phase 8C
- Phase 8C BigQuery project / region
- Phase 8C budget / cleanup plan
- CI state artifact 保存方式
- MCP Python SDK 最終選擇
- Codex MCP local configuration format
- 是否加入 optional OpenAI API adapter
- Phase 8C CI dataset cleanup 策略
- dbt package dependencies
- contract enforcement 的 adapter-specific behavior

每個決策必須：

- 記錄選項
- 記錄理由
- 記錄限制
- 實際測試
- 更新文件

---

# 22.1 External Constraint Review

下列規格依賴外部平台，開始實作前必須重新確認官方文件：

- BigQuery Sandbox limits
- BigQuery free-tier quotas
- DML / streaming availability
- dbt adapter compatibility
- Airflow Python compatibility
- Codex Skills format
- MCP SDK and security guidance

每次確認需記錄：

```text
source
checked_at
relevant_version
constraint_summary
impact_on_spec
```

若外部限制改變：

1. 停止依賴舊假設。
2. 更新本規格。
3. 說明 acceptance criteria 影響。
4. 不得用舊文件結果宣稱完成。

---

# 23. Final Principle

本 extension 的核心不是堆疊工具名稱，而是證明：

```text
可靠事件資料
→ 可治理的資料模型
→ 可重複的開發流程
→ 可驗證的品質與成本
→ 可查詢的 metadata 與 lineage
→ 受限制且有證據的 AI workflow
```

任何功能若無法：

- 重現
- 驗證
- 解釋
- 稽核
- 安全失敗

就不得視為完成。
