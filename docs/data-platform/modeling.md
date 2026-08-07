# dbt Modeling Semantics 與 Materialization

## 分層模型與 grain

| Layer／Model | Materialization | Grain／責任 |
|---|---|---|
| `stg_order_events` | View | 每個 `event_id` 一列；標準化 order event source |
| `stg_processed_events` | View | 每個 `consumer_group` × `event_id` 一列；處理與 Kafka 座標 metadata |
| `stg_log_metrics_minute` | View | 每分鐘 × service × endpoint 一列；標準化已聚合 log metrics |
| `int_order_event_sequence` | View | 每個 `event_id` 一列；加入 per-order Kafka stream sequence |
| `int_order_latest_state` | View | 每個 `order_id` 一列；latest known lifecycle state |
| `int_service_minute_metrics` | View | 每分鐘 × service × endpoint 一列；可重用 rate 邏輯 |
| `fct_order_events` | Table | 每筆 order event 一列 |
| `fct_orders` | Table | 每張訂單一列 |
| `mart_daily_sales` | Table | 每天 × 幣別 × 通路一列 |
| `mart_service_health` | Table | 每分鐘 × service 一列 |

## Order lifecycle

`valid_orders` 是每個 persisted event 一列，不是每張訂單一列。每個 `order_id` 的 deterministic sequence
使用 Kafka topic、partition 與 offset；只有遇到不合法的 duplicate coordinates 時，才以 `event_id` 作為
deterministic tie-breaker。Data tests 會拒絕重複 coordinate 或同一訂單跨 Partition。`event_time` 是可能
late arriving 的 business time，因此不用它選擇 latest state。

`latest_order_state` 是 Kafka stream 中目前已知的最新狀態，不是財務最終結算狀態。`payment_failed`
後仍可能出現 `order_paid`；由於來源不保存穩定 payment ID，多次 paid／failed events 都計入 payment
attempts。歷史上 paid 與 cancelled flags 可以同時為 true，latest state 則跟隨最後一筆 stream event。
來源無法證明 authorization、capture、refund、chargeback、final settlement、failure／cancellation reason
或 payment method。

所有 monetary products 都保留 currency；cross-event consistency tests 會拒絕同一訂單無聲改變幣別。
`mart_daily_sales` 依 date、currency、channel 分組，不做 FX conversion，`paid_amount` 也不是會計 revenue。

## Service health

來源 grain 是每分鐘 × service × endpoint。Endpoint rates 使用 guarded division；service totals 會先加總
counts 與 response-time numerators，再做除法：

```text
weighted_average_response_time_ms = sum(response_time_sum_ms) / sum(request_count)
```

實作不會平均 endpoint averages。`request_count = 0` 時，success rate、error rate 與 average response
time 都是 null。Tests 會拒絕 negative counters、重複 composite grain，以及 success、client-error、
server-error counts 加總不等於 request count 的資料列。

## Contract limitation

四個 published marts 都 enforce dbt Contracts。Contract metadata 將 monetary columns 宣告為 PostgreSQL
`numeric`，因為已驗證版本的 dbt-postgres Contract parser 無法安全 render 含逗號的 `numeric(18,2)` type。
Model SQL 仍將來源 monetary values cast 成 `numeric(18,2)`，因此保留 decimal semantics；precision／scale
是透過 SQL behavior 驗證，而不是只靠 Contract type string。

## Materialization 策略

`dbt/dbt_project.yml` 將 staging 與 intermediate 設為 View，marts 設為 Table。

- **staging View** 適合 rename、cast、UTC／日期標準化等輕量 source cleanup。它保留 source grain，
  不把 business metrics 混進入口層。
- **intermediate View** 適合可重用的事件排序、latest state 與 rate 邏輯，方便從最新來源查詢和除錯，
  也避免為每個中間步驟保存一份結果。
- **mart Table** 保存最近一次 `dbt build` 的計算結果，適合 Dashboard／BI 重複查詢，並承載 published
  Contract、docs、owner／SLO 與 tests。

View 儲存 SQL 定義而不是結果；查詢時仍會讀取 upstream relations 並使用 PostgreSQL CPU／I/O。Table
使用儲存空間並可能縮短重複查詢時間，但必須重新 build 才會反映新來源。新 Kafka event 寫入 `public`
後，staging／intermediate 查詢可以反映變化；`analytics_local_marts` 仍停留在最近一次
`make dbt-build` 的狀態。

這是目前 local-first 資料量、查詢模式、可讀性和實作簡潔性的選擇，不是 View／Table 的普遍效能定律。
未來若資料量或重建成本明顯增加，可以評估把特定 intermediate 改為 Table，或為有穩定 unique key、
late-arriving／backfill policy 與實際 warehouse evidence 的 mart 評估 incremental。Repository 目前沒有
實作 PostgreSQL incremental；Phase 8A metadata 中的 BigQuery strategy 只是 future physical-design policy，
不是已執行的 BigQuery model。

## Phase 7 scaffold boundary

Scaffold 是 draft generator，不是 schema inference system。它依 layer 正規化 model name，並提供 metadata
與 Contract structure，但不替開發者選 sources、upstream models、columns、business keys、monetary
semantics 或 transformations。未決事項保留為 `BLOCKING_TODO`，在開發者以驗證過的定義取代之前，
convention validation 會失敗。既有 Phase 6 model SQL 與 Contract semantics 不變。

## Phase 8A physical-design declarations

Mart metadata 中的 BigQuery fields 只描述 future physical design。Daily sales 與 service health 規劃以
bounded partition overwrite 保留現有 aggregate grains；order events 規劃以 `event_id` merge，同時保留
Kafka-coordinate lifecycle order。`fct_orders` 免除 partitioning，因為 mutable latest-state row 沒有穩定
calendar partition。這些宣告不改變 PostgreSQL materialization、Contract columns、money、currency grouping
或 lifecycle semantics，也從未在 BigQuery 執行。
