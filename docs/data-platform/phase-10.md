# Phase 10 Codex Skills and Incident Diagnosis

Phase 10 provides three repository-local Codex Skills and deterministic helpers. It consumes the
accepted Phase 9 restricted STDIO metadata adapter and preserves the Phase 1–9 boundary. No public
listener, paid API, cloud credential, Billing account, Airflow runtime, or manually registered Codex
MCP server is required for smoke/tests.

## Skills

`dbt-scaffold` validates a strict request, queries `get_model_schema` over STDIO, rejects absent
sources/columns and existing targets, and atomically writes SQL, contract/docs YAML, and unit-test
YAML. Its smoke uses the reserved `int_phase10_reserved_smoke` only in two temporary projects,
compares byte-identical output, verifies cleanup, and executes dbt parse, compile, and an affected
build in a temporary project. Production use must additionally execute convention validation and
the repository's affected tests; compile or build success is not business correctness.

`dbt-pr-review` emits only `blocking`, `warning`, or `suggestion` findings. The deterministic rules
cover layers, grain, direct sources, wildcard projection, docs/owner/SLO/contract/tests, column
removal/type change, incremental lookback, partition filters, multi-currency aggregation, division,
weighted averages, join risk, and lineage. Missing baseline or lineage is degraded evidence.

`incident-diagnosis` validates a bounded alert and calls Phase 9 asset, owner, quality, lineage,
pipeline, lag, and cost tools. Its normal state path is:

```text
RECEIVED → VALIDATED → ASSET_RESOLVED → QUALITY_CHECKED → LINEAGE_TRACED
→ PIPELINE_CHECKED → KAFKA_CHECKED → EVIDENCE_CORRELATED
→ DIAGNOSIS_PRODUCED → HUMAN_REVIEW_REQUIRED
```

Missing, partial, stale, failed required evidence adds `DEGRADED_DIAGNOSIS`. Optional Cloud cost
evidence being unavailable does not itself turn a local diagnosis into a cloud failure.

## Evidence and reports

Each incident evidence item records an ID, source/tool, evidence level, observed/generated time,
freshness status, tool status, and sanitized summary. Facts require evidence IDs. Hypotheses retain
high/medium/low confidence. Unknowns and rejected hypotheses remain separate. Fixture smoke is
`simulated`; the real transport demo is `static_validation` of local MCP consumption. Neither is
production observation.

Generated and Git-ignored reports are allowlisted below:

```text
reports/skills/dbt-scaffold-smoke.json
reports/skills/dbt-pr-review-smoke.json
reports/skills/dbt-pr-review-findings.json
reports/skills/incident-diagnosis-smoke.json
reports/skills/incident-demo-summary.json
reports/skills/phase10-ci-summary.json
reports/incidents/<incident_id>.json
reports/incidents/<incident_id>.md
```

Timestamps in deterministic fixtures come from the validated alert/request rather than wall clock.
Reports redact secret-shaped keys, connection strings, and bearer tokens. They contain no raw user
rows, raw environment dump, arbitrary artifact paths, or unrestricted logs.

## Commands

```bash
make skill-dbt-scaffold-smoke
make skill-dbt-pr-review-smoke
make skill-incident-diagnosis-smoke
make incident-demo
make test-skills
make phase10-ci
```

The scaffold/incident transport paths require a current Phase 9 metadata index. The aggregate CI job
creates fresh dbt and Phase 9 inputs before running these commands. All command failures propagate.

## Mutation boundary

The client interface only exposes `call(tool_name, arguments)` against the ten Phase 9 discovered
read-only tools. The incident diagnoser has no SQL, shell, filesystem-path, pipeline-rerun,
offset-reset, schema-mutation, IAM, cloud-resource, merge, or deployment method. Validation commands
are report strings for human review and are never executed by the agent. Mutation-safety tests verify
these methods do not exist and undeclared incident fields are rejected.

## CI artifacts

The `phase10` job depends only on accepted `phase9`. It uploads only run-specific copies under
`reports/phase10-ci/<run-id>/`: Skill summaries/findings, the controlled incident demo JSON/Markdown,
and sanitized metadata/lineage references. It excludes `.env`, `profiles.yml`, dbt target/cache,
credentials, unrelated logs, and previous-run reports.

## Known limitations

The deterministic reviewer uses conservative SQL patterns and still requires human semantic review.
The local metadata index is artifact-based rather than live production observability. Stale or
missing reports produce degraded results. Phase 8B is optional/not started and Phase 8C is
optional/deferred, so Sandbox/Cloud evidence remains unavailable. This is not production-ready and
does not provide autonomous remediation or an autonomous production agent.
