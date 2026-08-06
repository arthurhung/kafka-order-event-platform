# Phase 6 Data Platform Architecture

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
