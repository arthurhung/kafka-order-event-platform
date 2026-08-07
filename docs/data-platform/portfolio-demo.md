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
