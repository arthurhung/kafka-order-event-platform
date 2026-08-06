# Phase 8A Local BigQuery Compatibility and Cost Policy

Phase 8A is a mandatory local-only policy layer. It does not authenticate to Google Cloud, create
BigQuery resources, or execute a BigQuery query or dry run. It proves that partition, clustering,
scan-window, incremental-design, and cost policies are machine-readable and deterministically
checked. Passing this phase is not BigQuery runtime acceptance.

## Compatibility metadata

Every published model declares `warehouse_compatibility.bigquery: planned`. The source declaration
does not become `static_validated`; a fresh-manifest report computes that effective status only when
the policy passes. Phase 8A rejects Sandbox/Cloud statuses and observed evidence levels.

| Model | Partition | Cluster | Future strategy | Key | Late window | Max scan |
|---|---|---|---|---|---:|---:|
| `mart_daily_sales` | `event_date`, day | `currency`, `channel` | `insert_overwrite` | `event_date,currency,channel` | 7 days | 31 days |
| `mart_service_health` | `metric_date`, day | `service` | `insert_overwrite` | `metric_minute,service` | 2 days | 7 days |
| `fct_order_events` | `event_date`, day | `order_id`, `event_type` | `merge` | `event_id` | 7 days | 31 days |
| `fct_orders` | exempt | `order_id` | planned `merge` | `order_id` | 7 days | 31 days |

`fct_orders` is not partitioned because its latest-state timestamp changes when a new event arrives;
it has no stable calendar partition key. Its exemption has an owner, rationale, and expiring UTC
review date. The two aggregate marts use future partition overwrite designs, so nullable grouping
keys are not falsely presented as safe BigQuery merge keys. None of these future strategies execute
in Phase 8A.

## Static SQL policy

The validator uses a state-machine lexer before bounded token and pattern checks. It masks line and
block comments, strings, quoted identifiers, and backtick identifiers, and rejects unbalanced input.
It checks bounded partition predicates, wildcard projections, cartesian joins, currency grouping,
unsafe division, and pruning-hostile expressions. PostgreSQL-only syntax in the existing models is
reported as a warning while status remains `planned`; it is not rewritten merely to remove warnings.

Static analysis cannot prove that the BigQuery parser, optimizer, partition pruning, contracts, or
runtime will accept a query. Query examples are policy fixtures, not executed SQL.

## Cost policy and fixtures

Thresholds are integer bytes in `config/data_platform/bigquery_cost_policy.json`:

| Model | Warning | Block |
|---|---:|---:|
| `mart_daily_sales` | 256 MiB | 512 MiB |
| `mart_service_health` | 128 MiB | 256 MiB |
| `fct_order_events` | 512 MiB | 1 GiB |
| `fct_orders` | 128 MiB | 256 MiB |

The blocking threshold is inclusive. Waivers require an owner, reason, future UTC expiry, and
positive approved threshold. Partition and scan-window violations remain blocking. Missing estimates
remain null and are invalid; they are never changed to zero.

The `local_fixture` provider returns `evidence_level: simulated` and
`estimation_method: fixture_estimated`. `bigquery_sandbox` and `bigquery_cloud` return
`not_available` with all cloud measurements null. Providers never silently fall back.

## Orchestration boundary

Phase 8A uses a pure-Python orchestration contract with ordered tasks, bounded retries/timeouts,
quality-gate failure propagation, and no cloud tasks. An actual Python 3.14.6 installation attempt
for Airflow 3.3 failed because transitive `cryptography` and `libcst` source builds lacked usable
OpenSSL/pkg-config and Rust build prerequisites on the verified macOS environment. The project did
not downgrade Python, install system build tools, or add a stub. Reports therefore mark Airflow
runtime/import `not_available`; no DAG runtime claim is made. Real PostgreSQL-to-BigQuery
orchestration remains Phase 8C.

## Commands

```bash
make bigquery-static-validate
make bigquery-partition-policy
make bigquery-cost-policy
make bigquery-cost-report
make test-bigquery-policy
make phase8a-orchestration-validate
make test-phase8a-orchestration
make data-platform-phase8a-local
```

The aggregate runner creates a fresh dbt manifest in a run-specific ignored target directory. It
writes compatibility, partition/cluster, SQL, cost, fixture, policy-diff, orchestration, and summary
reports below `reports/data-quality/phase8a/<run-id>/`. A prior manifest or cost policy is optional;
missing prior state is named explicitly and is not presented as a completed diff.

GitHub Actions keeps the Phase 7 job unchanged and runs a separate dependent `phase8a` job. It uses
no GCP environment, account, API, dataset, or service-account key and uploads only local artifacts.

## Evidence and troubleshooting

- `static_validation`: manifest, metadata, SQL policy, diff, and orchestration checks.
- `simulated`: fixed local cost fixtures only.
- `not_available`: Sandbox and Cloud providers in this phase.
- A blocking finding returns non-zero and prevents a passed aggregate summary.
- An existing run ID is rejected rather than overwritten.
- Inspect the run-specific report and dbt log path; do not substitute stale `dbt/target` artifacts.

No GCP credentials were used. No Billing account was required. No BigQuery query or dry run was
executed. Fixture estimates are not BigQuery optimizer results. Phase 8B is optional Sandbox
observation; Phase 8C is deferred Billing-enabled cloud execution.
