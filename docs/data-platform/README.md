# Data Platform 文件導覽

這個目錄記錄 Phase 6～10 的 as-built Data Platform：PostgreSQL dbt data products、Paved Road／Slim CI、
Phase 8A local BigQuery policy、Metadata Index、唯讀 MCP 與 Codex Skills。功能範圍與驗收權威仍是
[`DATA_PLATFORM_SPEC.md`](../../DATA_PLATFORM_SPEC.md)；Kafka Core 則以 [`SPEC.md`](../../SPEC.md) 為準。

## 第一次看這個專案

1. [`README.md`](../../README.md)：先用 60 秒流程、完整資料流與 Quick Start 建立全貌和安全邊界。
2. [`architecture.md`](architecture.md)：理解 `streaming` Database、`public`／`analytics_local_*` Schemas、
   View／Table 與 Phase 7～10 的連接方式。
3. [`portfolio-demo.md`](portfolio-demo.md)：依 5 分鐘、15 分鐘或逐步驗證路徑實際操作。

## 想理解 Kafka 與可靠性

1. [`../architecture.md`](../architecture.md)：Topic、Partition、key、Consumer Group 與 Kafka Core 資料流。
2. [`../reliability.md`](../reliability.md)：manual offset commit、PostgreSQL transaction boundary、bounded
   retry、DLQ 與冪等性（idempotency）。
3. [`../demo.md`](../demo.md)：Consumer stop／restart、lag recovery、uncommitted replay 與 Demo report。
4. [`portfolio-demo.md`](portfolio-demo.md)：把 `make consumer-lag`、`make demo` 放進完整本機展示順序。

此專案是 at-least-once + idempotency，不宣稱 Kafka 與 PostgreSQL 的 distributed exactly-once。

## 想理解 dbt 分層與資料模型

1. [`architecture.md`](architecture.md)：先看 sources → staging → intermediate → marts 與實際 Schema。
2. [`modeling.md`](modeling.md)：再看 event／order／daily sales／service health grain、currency 與 weighted average。
3. [`quality.md`](quality.md)：理解 source、data、unit tests、freshness、Contract 與 evidence classification。
4. [`phase-6.md`](phase-6.md)：依 Phase 6 runbook 在 PostgreSQL 完成 dbt build 和 docs。

## 想理解 CI 與治理

1. [`phase-7.md`](phase-7.md)：dbt draft scaffold、convention validator、Contract diff、Slim CI 與 fallback。
2. [`phase-8a.md`](phase-8a.md)：local-only BigQuery compatibility、partition／cost policy 與 simulated evidence。
3. [`phase-9.md`](phase-9.md)：Metadata Index、lineage、read-only MCP、redaction、audit 與 degraded behavior。
4. [`phase-10.md`](phase-10.md)：Codex Skills、Incident Diagnosis、mutation boundary 與 Human Review。

GitHub Actions 的實際觸發條件、Job dependencies 與 steps 以
[`data-platform-ci.yml`](../../.github/workflows/data-platform-ci.yml) 為準。Workflow 是 CI，不是 deployment。

## 想理解 Metadata、MCP 與 Codex Skills

1. [`phase-9.md`](phase-9.md)：先理解 Metadata Index 的 inputs、10 個 allowlisted tools 與 bounded evidence。
2. [`phase-10.md`](phase-10.md)：再看 `dbt-scaffold`、`dbt-pr-review`、`incident-diagnosis` 如何消費唯讀 evidence。
3. [`interview-guide.md`](interview-guide.md)：最後整理成架構解釋、工程取捨與不能過度宣稱的能力。

## 想在本機實際操作

1. [`portfolio-demo.md`](portfolio-demo.md)：最完整的逐步命令、預期輸出、資料變更與清理說明。
2. [`Makefile`](../../Makefile)：所有支援的 local command surface；文件中的 target 必須以它為準。
3. [`README Quick Start`](../../README.md#quick-start逐層在本機測試)：依基礎環境、fixtures、dbt、metadata／
   MCP、Phase gates 與 reliability demo 分段執行。

操作情境要分清楚：

- **快速展示**：使用 5 分鐘或 15 分鐘 walkthrough，聚焦現成 local state 的代表性 evidence。
- **完整本機驗證**：按逐步表格執行，建立 fresh fixtures、dbt artifacts、Metadata 與 Skills reports。
- **Existing-state regression**：保留 volumes，重跑可重入 commands，驗證 idempotency 與現有狀態相容性。
- **Clean-room acceptance**：確認資料可刪除後才用 `docker compose down -v`，從空 volumes 驗證重建流程。

## Phase 狀態

Mandatory local portfolio mainline Phase 6 → 7 → 8A → 9 → 10 已 accepted。Phase 8B 是 optional／not
executed；Phase 8C 是 optional／deferred。任何 `sandbox_observed` 或 `cloud_observed` claim 都不適用於目前
repository evidence。
