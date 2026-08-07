# Portfolio Demo Guide

This guide presents the accepted local portfolio mainline. It does not execute Phase 8B or 8C and
does not require GCP credentials, Billing, BigQuery, Cloud Composer, or a paid AI API.

## Demo prerequisites

- Python 3.14.6 in the pyenv virtualenv `kafka_streaming`
- Docker with Docker Compose
- Free local ports 29092, 5432, and 8080
- A clean checkout with no unreviewed local changes
- Local `.env` and `dbt/profiles.yml` copied from committed examples
- No credentials in Git; environment-specific secrets remain local or in a CI secret store

One-time setup:

```bash
pyenv activate kafka_streaming
python --version
pyenv version
python -m pip install -e . --group dev --group data-platform
cp .env.example .env
cp dbt/profiles.yml.example dbt/profiles.yml
```

Start the reproducible local baseline:

```bash
make up
make topics
make migrate
make data-platform-fixtures
make dbt-deps
```

Expected:

- every command exits 0;
- Kafka and PostgreSQL report healthy;
- topic creation and migration are rerunnable;
- fixture output identifies a deterministic run without truncating tables or resetting offsets.

## Five-minute walkthrough

Run these commands from the repository root:

```bash
make dbt-build
make dbt-source-freshness
make dbt-docs
make metadata-index
make mcp-smoke
make skill-dbt-pr-review-smoke
make incident-demo
```

Expected:

1. `dbt-build` exits 0 and reports no model/test errors. Show the four contracted marts in
   `dbt/models/marts/marts.yml` and point out their different grains.
2. Freshness exits 0 because the fixture is current. `dbt-docs` generates ignored manifest/catalog
   artifacts; it does not start a web server.
3. `metadata-index` builds normalized assets and lineage from the current artifacts.
4. `mcp-smoke` exits 0, lists exactly ten allowlisted read-only tools, and executes a bounded search.
5. PR review produces deterministic findings with `blocking`, `warning`, or `suggestion` severity.
6. The incident demo uses real local STDIO transport, writes ignored reports under
   `reports/incidents/` and `reports/skills/`, and records `mutation_executed=false`.

An incident report may be degraded when lag, freshness, pipeline, or optional cloud evidence is
missing or stale. The report must name unknowns rather than invent a root cause.

## Fifteen-minute technical walkthrough

### 1. Kafka reliability

```bash
make describe-topics
make consumer-lag
```

Explain partition keys, fixed consumer groups, DB-before-offset commit, the replay window,
idempotency, bounded retry, and DLQ delivery confirmation. For the longer executable Kafka scenario,
use `make demo`; it generates a run-specific report and does not reset offsets or delete volumes.

### 2. dbt modeling, contracts, and tests

```bash
make dbt-build
make dbt-test
make dbt-source-freshness
make dbt-validate-conventions
```

Trace `public` sources through staging and intermediate models to marts. Contrast event grain
(`fct_order_events`) with order grain (`fct_orders`), and show that `mart_daily_sales` retains
currency while `mart_service_health` uses weighted latency.

### 3. Slim CI and breaking contracts

```bash
make dbt-contract-check
make dbt-slim-ci-local
```

The contract target verifies an identical-manifest pass and a removed-column fixture that must be
blocking. The overall Make target exits 0 only when the expected blocking scenario returns its
required non-zero result. Slim CI demonstrates native `state:modified+ --defer`; a missing base state
is explicitly labeled full-build fallback.

### 4. BigQuery policy boundary

```bash
make bigquery-static-validate
make bigquery-cost-policy
make phase8a-orchestration-validate
```

Show partition/clustering metadata, bounded partition predicates, currency safety, and byte
threshold decisions. Cost output is fixture-based `simulated`; orchestration is a pure-Python local
contract because Airflow runtime is `not_available`. No BigQuery request occurs.

### 5. Metadata and MCP

```bash
make metadata-index
make metadata-validate
make mcp-smoke
```

Show normalized assets, bounded lineage, schema lookup, quality lookup, explicit unavailable cloud
evidence, redaction, result limits, timeout behavior, and sanitized audit records.

### 6. Skills and incident diagnosis

```bash
make skill-dbt-scaffold-smoke
make skill-dbt-pr-review-smoke
make skill-incident-diagnosis-smoke
make incident-demo
```

The scaffold queries verified metadata and writes only temporary smoke projects. Review findings are
deterministic before generative interpretation. Incident output separates confirmed facts,
hypotheses, unknowns, and rejected hypotheses, then stops at human review.

## 逐步本機驗證

以下順序適合從既有 clone 驗證完整 local path。`reports/` 與 `dbt/target/` 中大部分產物受 Git ignore；
它們是當次 local evidence，不應誤認為 committed 或 cloud-observed evidence。

| 步驟 | 指令 | 在做什麼 | 會修改什麼 | 成功時去哪裡看 |
|---:|---|---|---|---|
| 1 | `make up` | 啟動 Kafka、PostgreSQL、Kafka UI 並等待服務 | 建立／啟動 containers 和 named volumes；不清資料 | `docker compose ps`；health check 應成功 |
| 2 | `docker compose ps` | 唯讀查看 Compose services 狀態 | 不修改資料 | Kafka、PostgreSQL、Kafka UI 的 status／health |
| 3 | `make topics` | 以 bootstrap script 可重入建立固定 Topics | Topic 不存在時建立；不刪除既有 Topic／messages | command exit 0 |
| 4 | `make list-topics` | 列出 Kafka Topics | 不修改資料 | 三個 `ecommerce.*.v1` Topic |
| 5 | `make describe-topics` | 顯示 Partition、RF 與配置 | 不修改資料 | orders／logs 各 6 partitions，DLQ 3 partitions，RF=1 |
| 6 | `make migrate` | 套用 Alembic migration 到 `streaming.public` | 建立或更新 source schema objects；不 truncate rows | Alembic upgrade exit 0；三張 source tables 可用 |
| 7 | `make data-platform-fixtures` | 啟動 Consumer、發佈 deterministic events、等待 durable persistence | 可能新增 Kafka events 和 `public` rows；相同 run ID 會跳過既有 IDs | stdout JSON 與 `reports/data-quality/phase6-fixtures-latest.json` |
| 8 | `make consumer-lag` | 讀取兩個固定 Consumer Groups 的 point-in-time lag | 不 reset offsets、不修改資料 | partition table 與 group total；backlog 應趨近 0 |
| 9 | `make dbt-deps` | 安裝 `dbt/packages.yml` 宣告的 packages | 更新 ignored `dbt/dbt_packages/` | dbt deps exit 0 |
| 10 | `make dbt-debug` | 驗證 profile 與 PostgreSQL 連線 | 可能執行 adapter connection checks；不建 models | `Connection test: OK` |
| 11 | `make dbt-build` | 建立 staging／intermediate／marts 並執行 tests | 建／更新 `analytics_local_*` Views／Tables 與 `dbt/target/` | dbt PASS summary；四個 marts 在 `analytics_local_marts` |
| 12 | `make dbt-source-freshness` | 依 source persisted timestamps 驗證 freshness | 更新 `dbt/target/sources.json` 等 artifacts；不改 source rows | freshness PASS；若 fixture 太舊需先重載新 run |
| 13 | `make dbt-docs` | 產生 dbt documentation artifacts | 更新 ignored `manifest.json`、`catalog.json` 等；不啟動 docs server | `dbt/target/manifest.json`、`catalog.json` |
| 14 | `make metadata-index` | 從固定 dbt artifacts 與 allowlisted reports 建立 index／lineage | 寫入 `reports/metadata/` 三個核心 JSON | `metadata-index.json`、`lineage-graph.json`、`index-summary.json` |
| 15 | `make metadata-validate` | 驗證 Metadata schemas、counts 與 lineage consistency | 寫入 `metadata-validation-report.json` | stdout status 與 `reports/metadata/metadata-validation-report.json` |
| 16 | `make mcp-smoke` | 以 repository-local STDIO child process 列出／呼叫唯讀 MCP | 寫 smoke report 與 sanitized audit；不需 UI registration | `mcp-smoke-report.json`、`mcp-audit.jsonl`；tool count = 10 |
| 17 | `make phase9-ci` | 執行 Metadata build／validation、MCP smoke／tests 與 Phase 9 summary | 更新 `reports/metadata/`；不部署、不查 live SQL | `security-report.json`、`phase9-ci-summary.json` |
| 18 | `make phase10-ci` | 執行三個 Skill smokes、incident demo、Skill／MCP tests 與 summary | 寫 `reports/skills/`、`reports/incidents/`；scaffold 只寫 temporary projects | `phase10-ci-summary.json` 與 `INC-DEMO-001.{json,md}` |
| 19 | `make demo` | 跑 mixed events、DLQ、Consumer stop／restart、lag recovery、uncommitted replay | 新增測試 Kafka／DB 資料與 run-specific report；不 reset offsets | `reports/runs/benchmark-custom-*.json` 最新檔案與 exit 0 |
| 20 | `make down` | 停止並移除 Compose containers／network | **保留** `kafka-data`、`postgres-data` named volumes | `docker compose ps` 不再顯示執行中服務 |

常見失敗原因包括 `.env`／`dbt/profiles.yml` 尚未由範例建立、Python 不是 3.14.6、ports 被占用、
Docker 尚未就緒、fixture 未在 freshness window 內，以及執行 Metadata／Skills gate 前缺少 fresh dbt
artifacts。任何 non-zero exit 都應先保留原始 output，不要用 fabricated report 或 0 補 missing evidence。

### 已有資料的環境怎麼測

Existing-state regression 會保留 Kafka／PostgreSQL volumes，不執行 `docker compose down -v`。可以重跑
`make topics`、`make migrate`、fixtures、dbt、Metadata 與 MCP，驗證 bootstrap、migration、fixtures 和
evidence builder 的可重入性，以及既有狀態相容性。

Fixtures 與 `make demo` 可能新增測試 events、source rows 和 reports；使用相同 fixture run ID 時，已持久化
event IDs 會被跳過。新來源資料進入 `public` 後，staging／intermediate Views 查詢可反映它，但 mart
Tables 要重新執行 `make dbt-build` 才會更新。這種測試適合日常 regression，不等於從空環境驗收。

### 何時需要乾淨環境

Clean-room acceptance 用於確認新的 clone／空 volumes 能完整重建，而不是日常必要步驟。只有在確認
本機 Kafka／PostgreSQL 資料可永久刪除後，才考慮：

```bash
docker compose down -v
```

> **警告：**這會停止 services 並刪除 Kafka／PostgreSQL named volumes，資料無法由 repository 自動復原。
> 執行前必須人工確認目標環境與資料保留需求。

合理情境包括最終 acceptance、驗證 fresh clone、修改 migration／Topic bootstrap 後重建、懷疑殘留資料
造成 false pass，或錄製正式 Demo。`make down` 只執行 `docker compose down`，停止 services 並保留
named volumes；兩者不可互換。

## Safe failure demonstrations

These commands use temporary paths or committed fixtures and do not mutate source tables or Kafka
consumer groups.

### Invalid or conflicting scaffold request

```bash
python -m pytest tests/data_platform/unit/test_scaffolding.py \
  -k 'conflicting_prefix or refuses_any_existing_target' -vv
```

Expected: tests pass by proving conflicting layers and overwrite attempts are rejected.

### Removed contract column fixture

```bash
make dbt-contract-check
```

Expected: the internal removed-column comparison returns the required blocking status; the wrapper
exits 0 only because that failure was expected and verified.

### Missing documentation fixture

```bash
python -m pytest tests/data_platform/unit/test_conventions.py \
  -k missing_mart_column_description -vv
```

Expected: the test passes by proving a mart column without a description is blocking.

### Malformed or prohibited MCP input

```bash
python -m pytest tests/data_platform/unit/test_metadata_service.py \
  -k invalid_sql_extra_fields_and_bounded_depth -vv
```

Expected: SQL-shaped search input, undeclared fields, and excessive lineage depth are rejected.

### Insufficient incident evidence

```bash
python -m pytest tests/data_platform/unit/test_phase10_incidents.py \
  -k insufficient_evidence -vv
```

Expected: status is degraded, confidence remains low, unknowns are listed, and no root cause or
mutation is fabricated.

## Cleanup

Safe cleanup that preserves Docker volumes and their data:

```bash
make down
```

Generated dbt targets, logs, profiles, and reports are intentionally ignored by Git. `make clean`
removes Python caches and `.coverage` only. This guide does not recommend `docker compose down -v`
because that deletes local Kafka/PostgreSQL volumes; use it only after independently reviewing and
accepting that data-loss risk.
