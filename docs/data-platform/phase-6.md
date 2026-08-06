# Phase 6 Local Runbook

## Setup

Use Python 3.14.6 and the repository pyenv environment:

```bash
python -m pip install -e . --group dev --group data-platform
cp .env.example .env
cp dbt/profiles.yml.example dbt/profiles.yml
make up
make topics
make migrate
```

`dbt/profiles.yml` and `.env` are local-only. For CI, select `DBT_TARGET=ci` and supply a unique,
non-public `DBT_TARGET_SCHEMA`. Both local and CI targets use the existing `public` source
schema.

## Build and validate

```bash
make data-platform-fixtures
make dbt-deps
make dbt-debug
make dbt-parse
make dbt-compile
make dbt-build
make dbt-test
make dbt-source-freshness
make dbt-docs
make test-data-platform
```

Generated dbt artifacts live under `dbt/target/`; logs and downloaded packages are also
ignored. `dbt docs generate` creates documentation artifacts but does not start a web server.

## Scope and limitations

Phase 6 is PostgreSQL-only and local-first. It includes no BigQuery, GCP, Airflow, GitHub
Actions, MCP server, Codex Skill, Agent, scheduler, BI dashboard, raw-log persistence, FX
conversion, or production claim. The Kafka producer/consumer reliability behavior, topics,
consumer groups, event schemas, migrations, transactions, offset commits, retries, and DLQ
behavior remain unchanged.
