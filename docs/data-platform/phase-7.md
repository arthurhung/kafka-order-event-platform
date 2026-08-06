# Phase 7 Paved Road and Slim CI

## Scaffold usage

Create a draft model from the repository root:

```bash
python scripts/data_platform/scaffold_dbt_model.py \
  --name daily_customer_orders \
  --layer marts \
  --owner data-platform \
  --domain commerce \
  --grain "one row per event_date and user_id"
```

The final name is `mart_daily_customer_orders`. Staging and intermediate requests receive `stg_`
and `int_`; a supplied conflicting prefix is rejected. If either target SQL or YAML exists, the
command returns non-zero without changing either file.

The output is deliberately a draft. Templates contain no guessed source, upstream dependency,
column, business key, or financial meaning. `BLOCKING_TODO` and `meta.scaffold_status: draft`
remain until a developer supplies verified SQL, columns, descriptions, constraints, and tests.
Convention validation blocks the draft. `make dbt-scaffold-smoke` exercises the CLI in a temporary
directory and never writes to the real model directory.

## Naming conventions

| Layer | Directory | Required prefix |
|---|---|---|
| staging | `dbt/models/staging/` | `stg_` |
| intermediate | `dbt/models/intermediate/` | `int_` |
| marts | `dbt/models/marts/` | `fct_`, `dim_`, or `mart_` (`mart_` for this scaffold) |

Names are lowercase snake case. `final_table`, `temp_data`, `new_model`, and `test_model` are
prohibited.

## Convention validator

`make dbt-validate-conventions` creates a fresh temporary manifest and checks:

- layer, directory, prefix, duplicate and prohibited names;
- mart grain, owner, domain, data product, maturity, PII flag, SLO, contract policy and contract;
- published column descriptions;
- direct `source()` dependencies only in staging and no multi-source staging model;
- wildcard projections: error in staging/marts, warning in intermediate;
- blocking scaffold TODOs.

Errors return non-zero. Warnings and informational findings remain in the JSON report without
turning a passing run into a failure.

## Contract change policy

The checker compares previous and current `manifest.json` files. Blocking changes include
published model/column removal, same-type rename candidates, incompatible type changes,
nullable-to-required changes, grain or business-key changes, contract removal, and required-test
removal. Required columns, owner/SLO, materialization, incremental key, and metric description
changes are marked potentially breaking and require manual review. Nullable documented columns
are non-breaking.

Every finding records model, optional column, change type, severity, classification, downstream
paths, evidence, and previous/current Git SHAs. Text changes are not treated as proof of semantic
equivalence. `make dbt-contract-check` verifies both a passing comparison and a fixture whose
removed published column must return exit code 1.

## Slim CI architecture

With a resolvable base revision, the runner:

1. extracts that revision into a temporary directory using `git archive`;
2. builds it in run-specific `analytics_ci_base_*` schemas;
3. stores its manifest and catalog under a unique ignored state directory;
4. parses the current project into a different target directory;
5. runs convention and contract checks;
6. obtains selection evidence with `state:modified+`;
7. executes `dbt build --select state:modified+ --defer --state <base-state>`;
8. generates the current catalog and records build and freshness artifacts plus a machine-readable
   summary;
9. cleans only its own `analytics_ci_base_*` and `analytics_ci_current_*` schemas.

`--defer` lets unselected current dependencies resolve to relations represented by the base
manifest. The runner never drops, truncates, or writes models into `public`.

If the base revision cannot be resolved, the runner records `mode: full_ci_fallback`, reports the
previous contract state as unavailable, and runs a complete current `dbt build`. It never reports
this path as normal state-based CI.

## Local commands

```bash
make dbt-scaffold-smoke
make dbt-validate-conventions
make dbt-contract-check
make dbt-slim-ci-local
```

The Slim CI target also runs a temporary-project integration scenario that changes
`stg_order_events`, checks its downstream selection, and executes the selected build with defer.
It then demonstrates both state mode and full fallback.

## GitHub Actions workflow

`.github/workflows/data-platform-ci.yml` uses commit-pinned official checkout, Python setup, and
artifact upload actions. It starts PostgreSQL as a service and only the Kafka service required by
the existing fixture path; Kafka UI is not started. The workflow runs migrations, deterministic
fixtures, policy scenarios, Slim CI, Python checks, and uploads diagnostics even when a step fails.

The workflow does not consume ignored local state. Pull requests use the base SHA; pushes use the
previous SHA when available and otherwise take the explicit full fallback.

## Artifacts

Generated, ignored artifacts include:

- base/current manifest and current catalog/run/freshness results;
- preserved Slim build run results;
- convention report;
- contract diff report and separately named pass/breaking fixture reports;
- modified-staging selection evidence;
- state-mode and fallback summaries.

Artifacts contain sanitized bounded command output. Passwords and connection strings are not
included.

## Limitations and troubleshooting

- A scaffold is incomplete until all blocking TODOs are resolved.
- Manifest comparison cannot prove all business semantic changes; manual review remains required.
- The first CI run may not have previous state and therefore performs a full build.
- Local Slim CI needs the existing ignored `dbt/profiles.yml`, running PostgreSQL, Kafka for fixture
  loading, and fresh fixtures before freshness validation.
- A GitHub workflow file existing locally is not GitHub execution evidence. Phase 7 acceptance
  requires a committed, pushed, observed successful run and its required artifacts.
- If a run fails, inspect `reports/data-quality/phase7-*.json` and its unique
  `dbt/target/phase7-ci/<run-id>/` directory. Do not substitute unrelated stale artifacts.
