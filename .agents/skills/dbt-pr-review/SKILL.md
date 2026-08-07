---
name: dbt-pr-review
description: Review dbt changes with deterministic contract, convention, SQL safety, cost, documentation, testing, and lineage checks. Use when Codex is asked to review dbt changes, a data-model PR, or a published contract change in this repository.
---

# dbt PR Review

## Invocation conditions

Use for dbt change or PR review. Do not silently modify reviewed files.

## Required inputs

Require changed model SQL/YAML or a repository diff, the current manifest, and the previous manifest
when available. Missing baseline or lineage is degraded evidence, never proof of safety.

## Required context

Read `AGENTS.md`, Phase 10 of `DATA_PLATFORM_SPEC.md`, and every file in `references/`. Query Phase 9
`get_downstream_impact` for lineage when available.

## Allowed tools

Read repository/dbt artifacts, call Phase 9 read-only metadata tools, and run only the fixed helper
scripts in this Skill plus repository dbt validators/tests.

## Prohibited actions

Do not approve or merge a PR, push code, alter production, execute arbitrary SQL/shell, silently fix
findings, suppress blocking findings, or let generative judgment override a deterministic finding.

## Execution steps

1. Detect changed models deterministically.
2. Compare published contracts against the baseline.
3. Validate layer, grain, dependencies, explicit projection, descriptions, owner, SLO, contract,
   required tests, incremental lookback, partition filters, money/currency, safe division, weighted
   averages, join explosion, and duplicated logic.
4. Query downstream lineage impact or mark it degraded.
5. Sort all findings by stable keys and render JSON plus a human summary.

## Validation steps

Run `make skill-dbt-pr-review-smoke` and `make test-skills`. Verify normal, warning, blocking,
contract-removal, type-change, documentation, incremental, partition, multi-currency, division, and
missing-baseline fixtures.

## Expected output

Emit findings with exactly `blocking`, `warning`, or `suggestion` severity and fields `file`, `model`,
`rule`, `reason`, `impact`, `recommendation`, and `evidence`.

## Failure handling

Malformed input fails closed. Missing baseline or lineage yields explicit degraded/unavailable
findings. Lack of evidence never means no issue.

## Completion criteria

Complete when deterministic helpers ran, findings are stable and evidence-linked, blocking changes
remain blocking, and no repository or external state was mutated.
