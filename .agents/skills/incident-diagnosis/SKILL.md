---
name: incident-diagnosis
description: Diagnose data freshness, quality, pipeline, or Kafka-lag incidents from restricted read-only Phase 9 MCP evidence. Use when Codex is asked to investigate or explain a data incident and produce remediation, backfill, or validation plans for human review.
---

# Incident Diagnosis

## Invocation conditions

Use for data incident diagnosis only. Use degraded mode whenever required evidence is unavailable,
stale, partial, or a tool fails.

## Required inputs

Require `incident_id`, `alert_type`, `asset` or `pipeline`, `observed_at`, `severity`, and `message`.
Reject undeclared fields, paths, SQL, and shell-shaped content.

## Required context

Read `AGENTS.md`, Phase 10 of `DATA_PLATFORM_SPEC.md`, and all three files in `references/`. Use the
fixtures only for deterministic demo/tests; never present them as observed production evidence.

## Allowed tools

Call only Phase 9 read-only tools: asset search, schema/owner, lineage/impact, quality, pipeline
failures, consumer lag, and cost estimate. Generate reports and recommendations for human review.

## Prohibited actions

Never execute SQL or arbitrary shell, read arbitrary paths, mutate schemas/data/IAM/cloud resources,
rerun pipelines, reset offsets, alter consumer groups, merge/push/commit, deploy, or acknowledge an
incident for a human.

## Execution steps

1. Validate the alert.
2. Traverse `RECEIVED → VALIDATED → ASSET_RESOLVED → QUALITY_CHECKED → LINEAGE_TRACED →
   PIPELINE_CHECKED → KAFKA_CHECKED → EVIDENCE_CORRELATED → DIAGNOSIS_PRODUCED →
   HUMAN_REVIEW_REQUIRED`.
3. Add `DEGRADED_DIAGNOSIS` if any required evidence is missing, stale, partial, or failed.
4. Inventory evidence with ID, source/tool, evidence level, time, freshness, status, and sanitized
   summary.
5. Separate confirmed facts, hypotheses, unknowns, and rejected hypotheses.
6. Produce affected assets, possible cause, remediation, backfill, validation, and approval items.
7. Write JSON and Markdown reports without raw logs, secrets, PII, or absolute user paths.

## Validation steps

Run `make skill-incident-diagnosis-smoke`, `make incident-demo`, and `make test-skills`. Ensure at
least one test performs real STDIO initialize, tools/list, and tools/call against Phase 9.

## Expected output

Write `reports/incidents/<incident_id>.json` and `.md` with stable ordering and a deterministic
generated time from the validated alert/fixture.

## Failure handling

Tool failure or stale/partial evidence produces degraded status and explicit unknowns. Never fill
missing values with zero and never fabricate a root cause.

## Completion criteria

Complete when all evidence is traceable, confidence is high/medium/low, human approvals are explicit,
mutation-safety remains enforced by the exposed interfaces, and reports validate in both formats.
