---
name: dbt-scaffold
description: Generate a repository-conforming dbt staging, intermediate, or mart model from verified Phase 9 metadata. Use when Codex is asked to create or implement a dbt model, its contract, tests, or documentation in this repository.
---

# dbt Scaffold

## Invocation conditions

Use this Skill for every request to generate or implement a dbt model. Do not use it for review-only work.

## Required inputs

Require the business requirement, model grain, expected consumers, owner, domain, required metrics,
freshness target, upstream asset, and selected columns. Require an explicit grain beginning with
`one row per` for intermediate and mart models.

## Required context

Read `AGENTS.md` and the Phase 10 section of `DATA_PLATFORM_SPEC.md`. Read
`references/modeling-conventions.md`, `references/quality-requirements.md`, and
`references/contract-policy.md`. Inspect actual source/model columns through Phase 9
`get_model_schema`; if STDIO is unavailable, inspect only the same allowlisted metadata artifacts
and label the result degraded.

## Allowed tools

- Read repository documentation, dbt files, and allowlisted Phase 9 metadata artifacts.
- Call Phase 9 read-only metadata tools over restricted STDIO.
- Run `scripts/inspect_available_columns.py`, `scripts/scaffold_model.py`, and
  `scripts/validate_generated_model.py`.
- Run fixed dbt parse, compile, affected build, test, and documentation commands.

## Prohibited actions

Do not guess columns, sources, financial semantics, currency conversion, or business correctness.
Do not overwrite an existing model. Do not accept a path from a model name. Do not mutate source or
production schemas, rerun production, commit, push, merge, deploy, or hide a failed test.

## Execution steps

1. Validate the strict request schema.
2. Confirm business requirement, grain, consumers, owner, domain, metrics, and freshness.
3. Query real upstream schema and evidence through Phase 9.
4. Stop before generation if a source/model/column is absent.
5. Select the narrowest correct staging, intermediate, or mart layer.
6. List the files to create and refuse every existing target.
7. Generate SQL, YAML contract/documentation, and unit-test YAML atomically.
8. Run convention validation, dbt parse, dbt compile, affected dbt build, and tests.
9. Record every command, exit code, warning, evidence ID, and human-review item.

## Validation steps

Run `make skill-dbt-scaffold-smoke`, then run the dbt parse/compile/affected build commands recorded
by the helper. Treat compile as syntax/graph evidence, not business correctness. Any failed command
keeps status failed.

## Expected output

Produce a stable JSON report containing request, selected sources/columns/layer, grain, generated
files, validation commands and exit codes, evidence IDs, warnings, assumptions, human-review items,
and deterministic status.

## Failure handling

On missing metadata, missing grain, traversal, invalid identity, overwrite, or failed dbt validation,
stop and report the exact blocking reason. Atomic generation must leave no partial model files.

## Completion criteria

Complete only when verified columns were used, files are internally consistent, convention checks,
parse, compile, affected build, and tests pass, and unresolved business assumptions are called out.
Never commit automatically.
