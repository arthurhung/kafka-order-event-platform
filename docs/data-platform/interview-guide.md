# Interview Guide

## 30-second introduction

這個作品集從一個具備 manual offset commit、冪等、retry、DLQ 與 PostgreSQL transaction boundary 的
Kafka 事件平台出發，再往上建立 dbt 分層資料產品、contracts、quality gates、Slim CI，以及由 dbt
artifacts 產生的 metadata 與 lineage。最上層用唯讀 MCP 把固定、可稽核的證據提供給 Codex Skills，讓
AI 可以協助 scaffold、PR review 和 incident diagnosis，但所有 mutation 都停在人工核准邊界。整條必做
主線可在本機重現，不宣稱 production-ready，也不把模擬的 BigQuery evidence 說成真實雲端執行。

## 2-minute architecture explanation

Kafka Core 解決的是可靠事件處理：同一 order 以 key 維持 partition 內順序，consumer 在 PostgreSQL
transaction 成功後才 commit offset，並用同 transaction 的 idempotency marker 處理 replay。這層留下的
三張 `public` tables 是穩定、唯讀的 Data Platform interface。

dbt 把 operational event rows 轉成可使用的資料產品。PostgreSQL 是 local-first baseline，因為它讓
sources、marts、contracts、tests、freshness 與 docs 都能在 clean checkout 重現，不需要雲端帳號。事件
層與訂單層刻意分開：`fct_order_events` 保留 event grain，`fct_orders` 才是 one row per order 的 latest
known state；money 永遠保留 currency，沒有 FX 就不做跨幣別加總。

BigQuery 分成 8A/8B/8C 是為了讓 claim 與 evidence 對齊。8A 是必做的 local compatibility、partition
filter 與 simulated cost policy；8B 是選做 Sandbox observation；8C 才是需要 Billing 的真實 load、
`MERGE`、incremental 與 orchestration。這個 repository 只完成 8A，因此不能聲稱跑過 BigQuery。

dbt artifacts 與 reports 先經 deterministic code 正規化成 metadata index，再透過 10 個 allowlisted、
read-only STDIO MCP tools 提供 schema、lineage、quality、lag 與 cost evidence。Codex 負責理解需求、選工具、
關聯證據和產生建議；schema validation、contract diff、lineage traversal、policy decision 與 severity 則由
deterministic code 決定。任何 code change、pipeline rerun、backfill、offset reset、deploy 或 IAM 變更都需要
human review/approval，而且第一版根本沒有提供 production mutation interface。

## Key engineering decisions

### Manual commit and idempotency

DB commit 先於 Kafka offset commit。兩者之間的 crash window 會 replay，所以
`processed_events` marker 與 business write 必須在同一 PostgreSQL transaction。這提供 at-least-once
加 idempotency，不是跨系統 exactly-once。

### Event grain versus order grain

`valid_orders` 與 `fct_order_events` 是 one row per persisted event；`fct_orders` 才是 one row per
`order_id`。Latest state 依 Kafka coordinate 排序，不把可能 late 的 business event time 當 stream order，
也不把 latest known state 說成財務最終狀態。

### Multi-currency handling

Money 使用 Decimal/numeric，daily sales 以 currency 分組。沒有 FX rate、refund 或 settlement data，
所以不輸出 consolidated GMV、net revenue 或正式財務認列。

### Contract enforcement

四個 published marts 有 enforced contracts、column docs、tests、owner 和 SLO。Manifest contract diff 對
column removal、incompatible type、grain/business-key change 等回 blocking；無法 deterministic 證明的
semantic change 保留 human review。

### Slim CI

有 base state 時使用 dbt 原生 `state:modified+ --defer`，只 build 變更 model 與 downstream；base 不可用
時明確標示 full-build fallback。Base/current 使用隔離 schema，cleanup 只允許 run-specific prefix，永遠不
匹配 `public`。

### Evidence classification

`static_validation`、`simulated`、`sandbox_observed`、`cloud_observed`、`not_available` 不可互相升級。
缺少 bytes/job ID 不用 0 填補。Local fixture 永遠不是 BigQuery observation。

### Fail-closed MCP allowlist

MCP 只有十個固定 metadata tools，Pydantic 拒絕 extra fields 與 SQL-shaped input，lineage depth、node count、
response size 和 timeout 都有上限。它不接受 arbitrary path/URL，不執行 SQL/shell，也沒有 filesystem-write
tool；audit 只記 sanitized arguments/results metadata。

### Degraded diagnosis

Incident diagnosis 只有證據充分時才建立 fact；inference 必須附 confidence。必要 artifact 缺少、過時或
tool failure 時輸出 degraded diagnosis 與 unknowns，不用時間巧合硬說 root cause。Optional cloud evidence
unavailable 不是 cloud failure。

### Mutation safety

Incident client 只有對 read-only tools 的 `call`。Remediation、backfill 和 validation commands 只是供人
review 的文字，不會執行。Tests 驗證不存在 rerun、reset-offset、schema/IAM mutation 或 deploy methods，
demo report 固定記錄 `mutation_executed=false`。

## Honest limitations

面試時可以直接說：

- 「這是 local-first portfolio，不是 production system。單 broker、RF=1 與 local PostgreSQL 沒有 HA、
  backup、TLS/ACL、跨區或企業容量證據。」
- 「Phase 8A 只證明 BigQuery physical-design policy 可被本機 deterministic 檢查；我沒有跑 Sandbox 或
  Billing-enabled BigQuery，所以沒有 `MERGE`、incremental、partition runtime 或 Cloud Composer evidence。」
- 「MCP metadata 來自 artifacts，不是 live observability。Artifact 過時或缺少時，我選擇 degraded result。」
- 「Deterministic checks 可以抓 contract、convention 與部分 SQL risk，但不能證明 business semantics；
  dbt compile 也不等於 business correctness。」
- 「Incident Agent 不會自動修復。它只整理 evidence 和提出需要人核准的 plan。」
- 「Benchmark 是特定 host/workload 的結果，而且原始 Stress 曾失敗；producer delivery throughput 不能當
  end-to-end durable capacity。」
- 「這不是 enterprise governance、production incident management 或 autonomous agent platform。」

## Suggested live demo order

1. 打開 README 的 architecture 與 Phase status，先建立 claim boundary。
2. 開 `dbt/models/marts/marts.yml`，比較 event/order/service/sales grains、contracts、owner 與 SLO。
3. 執行 `make dbt-build`，再用 `make dbt-contract-check` 展示 breaking column 被阻擋。
4. 執行 `make dbt-slim-ci-local`，說明 state selection、defer 與 fallback。
5. 執行 `make mcp-smoke`，展示固定十個 read-only tools 和 schema validation。
6. 執行 `make skill-dbt-pr-review-smoke`，展示 deterministic severity 與 evidence。
7. 執行 `make incident-demo`，打開 ignored incident JSON/Markdown，指出 facts、unknowns、degraded 狀態與
   `mutation_executed=false`。
8. 最後回到 benchmark failed Stress report與 Important limitations，說明如何避免過度宣稱。

逐行命令與預期結果見 [Portfolio Demo Guide](portfolio-demo.md)。
