# Phase 6 Data Platform Architecture

Phase 10 adds a Codex AI harness above Phase 9: repository-local `dbt-scaffold`, `dbt-pr-review`, and
`incident-diagnosis` Skills use deterministic helpers and the restricted local STDIO adapter. The
incident composition root is read-only and produces evidence reports and human-review plans; it has
no production mutation interface. See `phase-10.md` for the state machine and transport boundary.

Phase 6 adds a local PostgreSQL dbt project without changing the Kafka Core. The three
`public` relations remain read-only sources and dbt writes only to schemas derived from
`DBT_TARGET_SCHEMA`:

```text
public.valid_orders          -> analytics_<target>_staging.stg_order_events
public.processed_events      -> analytics_<target>_staging.stg_processed_events
public.log_metrics_minute    -> analytics_<target>_staging.stg_log_metrics_minute
                                      |
                                      v
                         analytics_<target>_intermediate
                                      |
                                      v
                              analytics_<target>_marts
```

The local default is `analytics_local`; dbt's schema generation therefore creates
`analytics_local_staging`, `analytics_local_intermediate`, and `analytics_local_marts`.
CI uses target `ci` and must supply a unique `DBT_TARGET_SCHEMA`, such as
`analytics_ci_12345`. The committed profile example reads all connection values from
environment variables. A developer copies it to ignored `dbt/profiles.yml`; passwords and
complete connection strings must never be committed.

The project uses `dbt-core` 1.12 and `dbt-postgres` 1.11 in the `data-platform` dependency
group. This keeps dbt optional for the Kafka applications, while allowing one Python 3.14.6
environment to run the local completion gate. Compatibility is accepted only from actual
installation and execution, not from version metadata alone.

No raw application-log rows exist in PostgreSQL. Service models start from endpoint-level,
minute aggregates and cannot expose request IDs, client IP analytics, HTTP methods, or
individual latency distributions.

## Phase 7 developer path

Phase 7 adds deterministic developer tooling around the Phase 6 project without changing its
models or source ownership:

```text
draft scaffold -> dbt parse -> convention validation -> contract comparison
                                                     -> state:modified+ build
base revision -> isolated base schemas -> manifest --^       |-- defer upstream
```

The base revision is extracted with read-only Git archive into a temporary directory and built
in run-specific `analytics_ci_base_*` schemas. Current selected relations use separate
`analytics_ci_current_*` schemas. Unselected dependencies resolve through dbt defer to the base
relations. Cleanup is allowlisted to those run-specific prefixes and never matches `public`.

When a base revision cannot be resolved, the runner records `full_ci_fallback` and performs a
complete current build. State and diagnostic artifacts are written below ignored `dbt/target/`
and `reports/data-quality/`; existing local artifacts are not treated as previous CI state.

## Phase 8A local policy path

Phase 8A adds a separate local policy path around a fresh dbt manifest. It validates published-model
metadata and bounded query fixtures, evaluates fixed cost fixtures, records policy changes, and uses
a pure-Python orchestration contract to propagate quality-gate failures. Its GitHub Actions job runs
after but does not alter the Phase 7 job. No BigQuery adapter, GCP provider, cloud task, or production
scheduler is part of this path.

## Phase 9 metadata discovery path

Phase 9 reads fixed dbt, quality, contract, orchestration, lag, cost, and benchmark artifacts into a
deterministic asset/evidence index and lineage graph. A local STDIO MCP adapter exposes only bounded,
read-only metadata tools. It never reads source rows or invokes SQL, shell, pipeline, schema, or Kafka
mutation interfaces. Missing optional cloud artifacts degrade individual responses without disabling
the PostgreSQL/dbt metadata path. Phase 10 reasoning and Skills remain outside this layer.
