# AGENTS.md

## Repository Purpose

This repository contains two connected development tracks with independent
functional specifications.

### Track A — Kafka Event Processing Core

- Phases 1–5
- Specification: `SPEC.md`
- Stable MVP baseline
- Owns Kafka ingestion, processing reliability, and persistence semantics

### Track B — Data Platform and AI Harness

- Phases 6, 7, 8A, 8B, 8C, 9, and 10
- Specification: `DATA_PLATFORM_SPEC.md`
- Builds analytics, governance, developer tooling, and AI workflows on top of
  the Kafka Core

The tracks are connected through stable database, report, and artifact
interfaces. Data Platform work must not silently alter the Kafka Core.

## Specification Authority

Before planning or implementing work:

- Phase 1–5: read `SPEC.md`.
- Phase 6–10: read `DATA_PLATFORM_SPEC.md`.
- Cross-track changes: read both specifications.
- Repository-wide documentation changes: read both specifications.

The active phase specification is the source of truth for:

- scope;
- non-goals;
- deliverables;
- acceptance criteria;
- completion gates;
- safety boundaries.

`AGENTS.md` does not replace either functional specification. It defines how
Codex works in this repository and the limits on that work. If this file and
the active specification appear to conflict, stop and report the conflict. Do
not select the more permissive interpretation.

Requirement priority is:

1. explicit user instruction;
2. the active specification;
3. `AGENTS.md`;
4. existing repository conventions.

## Phase Routing

| Phase | Track | Specification | Requirement |
|---|---|---|---|
| 1–5 | Kafka Core | `SPEC.md` | Existing baseline |
| 6 | Data Platform | `DATA_PLATFORM_SPEC.md` | Mandatory |
| 7 | Data Platform | `DATA_PLATFORM_SPEC.md` | Mandatory |
| 8A | Data Platform | `DATA_PLATFORM_SPEC.md` | Mandatory |
| 8B | Data Platform | `DATA_PLATFORM_SPEC.md` | Optional, no Billing |
| 8C | Data Platform | `DATA_PLATFORM_SPEC.md` | Optional, deferred, Billing required |
| 9 | Data Platform | `DATA_PLATFORM_SPEC.md` | Mandatory |
| 10 | Data Platform | `DATA_PLATFORM_SPEC.md` | Mandatory |

Rules:

- The user must explicitly specify a phase before implementation begins.
- If the user says only "next step," determine the next legal phase from
  accepted evidence and propose a plan; do not implement it yet.
- Do not skip a mandatory prerequisite that has not passed acceptance.
- Phase 8B and Phase 8C are not prerequisites for Phase 9 or Phase 10.
- Phase 8C being `deferred` does not mean it is `blocked`.
- Track Phase 8A, 8B, and 8C separately. Do not collapse them into an ambiguous
  single Phase 8 status.
- The mandatory local portfolio mainline is Phase 6 → Phase 7 → Phase 8A →
  Phase 9 → Phase 10.

## Default Behavior When No Phase Is Specified

If the user does not explicitly specify a phase, Codex may only:

- inspect the repository;
- read documentation;
- analyze current status;
- identify gaps;
- propose a plan;
- list affected files;
- describe risks;
- describe validation steps.

Codex must not:

- modify files;
- add dependencies;
- implement features;
- change schemas;
- create cloud resources;
- start the next phase.

If the user requests a code review, acceptance review, or tests without naming
a phase, Codex may perform read-only inspection and the explicitly requested
tests. Do not expand that request into implementation.

## Planning Before Implementation

Before starting any phase, report:

1. Active Phase.
2. Active Specification.
3. Current repository status.
4. Prerequisite status.
5. Files expected to change.
6. Dependencies expected to change.
7. Risks and assumptions.
8. Tests and validation commands.
9. Completion Gate.
10. Explicit non-goals.

Wait for user confirmation before implementation. If the user explicitly asks
for direct implementation, first provide a concise scope, affected-file list,
validation plan, and prohibited work in the same response, then implement only
the named phase.

Prefer the simplest implementation that satisfies the active specification.
Do not create placeholders that falsely appear complete, silently expand scope,
or report success without executing the relevant command or test.

## Stop-at-Phase Boundary

After completing the requested phase:

- run only that phase's completion gate;
- report the results;
- list remaining failures;
- stop.

Do not automatically:

- start the next phase;
- add future-phase scaffolding;
- add optional cloud integrations;
- add MCP tools;
- add Skills;
- add Agent behavior;
- commit;
- push;
- merge.

This rule applies even when the next phase appears small or closely related.

## Local Python Environment

- Required Python version: `3.14.6`.
- The local interpreter is managed by pyenv.
- The project virtualenv is `kafka_streaming`, created from Python `3.14.6`.
- The repository `.python-version` must contain `kafka_streaming`.
- `pyproject.toml` must use `requires-python = ">=3.14,<3.15"`.
- Prefer `python`, `python -m pip`, and project Makefile commands after
  `pyenv local kafka_streaming`.
- Do not hard-code a user-specific interpreter path in application code,
  Docker files, scripts, or shared configuration.

Before implementation, verify:

```bash
python --version
pyenv version
python -c "import sys; print(sys.executable)"
```

Verify dependency compatibility with Python 3.14 by actual installation and
test execution. Do not silently downgrade Python or replace a dependency.
Report incompatibility first.

## Track A Scope and Phase Summary

The stable Kafka MVP includes Python 3.14.6, single-node combined KRaft Kafka,
PostgreSQL, Kafka UI, the Python event generator, order and application-log
consumers, Pydantic validation, manual Kafka offset commits, database
idempotency, bounded retry, DLQ handling, structured JSON logging, benchmark
and lag tools, Docker Compose, a Makefile, migrations, tests, and documentation.

Phase responsibilities remain defined by `SPEC.md`:

- Phase 1: project skeleton and local infrastructure.
- Phase 2: event models and generator.
- Phase 3: order consumer, idempotency, retry, and DLQ.
- Phase 4: application-log consumer and minute aggregation.
- Phase 5: benchmark, consumer lag, demo, and documentation.

Do not introduce out-of-scope technologies such as Flink, Spark, Debezium,
Kafka Connect, Schema Registry, ClickHouse, Doris, Hologres, Prometheus,
Grafana, Kubernetes, Terraform, multi-broker Kafka, or claims of exactly-once
processing across Kafka and PostgreSQL unless an explicit approved phase or
specification change requires them. Cloud work is permitted only within the
approved boundaries of Phase 8B or Phase 8C.

## Kafka Core Backward Compatibility

Data Platform work must not silently change:

- Kafka topic names;
- partition counts;
- message keys;
- event schema versions;
- consumer group names;
- manual offset commit timing;
- idempotency keys;
- retry classification;
- retry counts;
- retry backoff behavior;
- database transaction boundaries;
- DLQ behavior;
- graceful shutdown behavior;
- benchmark definitions;
- consumer lag definitions;
- existing report filenames referenced by documentation.

If a cross-track change is genuinely required:

1. Stop implementation.
2. Explain why it is required.
3. List compatibility and downstream impact.
4. Provide a migration plan.
5. Provide a rollback plan.
6. Propose updates to both `SPEC.md` and `DATA_PLATFORM_SPEC.md` and all
   affected tests and documentation.
7. Obtain explicit user approval before making the change.

## Kafka Reliability Rules

These rules are mandatory:

1. Disable automatic offset commit and automatic offset storage where required
   by `SPEC.md`.
2. Commit Kafka offsets only after downstream processing succeeds.
3. Store the idempotency record and business data in the same PostgreSQL
   transaction.
4. If PostgreSQL processing fails, do not commit the Kafka offset.
5. Send permanent decoding and validation errors to the DLQ.
6. Commit the original offset only after DLQ production succeeds.
7. A poison message must not permanently block its partition.
8. All retries must have a maximum attempt count and bounded exponential
   backoff.
9. Duplicate events must not create duplicate business records.
10. Do not claim exactly-once delivery or production readiness.
11. Do not describe `acks=all` on a single-node cluster as multi-replica
    durability.

## Source Data Ownership

Phase 1–5 components own writes to:

- `public.valid_orders`;
- `public.processed_events`;
- `public.log_metrics_minute`.

Data Platform components must treat these tables as read-only. dbt and
analytics code must:

- write only to isolated analytics schemas;
- never truncate or mutate source tables;
- never use hooks to alter Kafka Core data;
- never change idempotency markers.

Use only real available source tables and columns:

- PostgreSQL currently contains no raw application-log rows.
- The only stored log dataset is the minute-level `log_metrics_minute`
  aggregate.
- Do not name or document a model in a way that implies raw logs are stored.
- `valid_orders` does not store `payment_id`, `payment_method`,
  `failure_code`, `failure_reason`, or `cancellation_reason`.
- Do not invent raw tables, source columns, payment details, refund data, or
  other unavailable business semantics.

## Database, Event, and Application Rules

### Database

- Use SQLAlchemy 2.x style and Alembic migrations.
- Use UTC and timezone-aware datetime values.
- Use `Decimal`/`numeric` for money; do not use Python `float` for monetary
  database writes.
- Keep transaction boundaries explicit.
- Repository functions must not silently commit unless their contract clearly
  states it.
- Idempotency and business writes must share one transaction.
- Add only indexes required by the active specification's query patterns.

### Events and schemas

- Use Pydantic 2.x models.
- Every event includes `event_id`, `event_type`, `event_version`, `event_time`,
  `source`, and `payload`.
- Newly generated events use globally unique `event_id` values; duplicate
  injection intentionally reuses an existing ID.
- Event timestamps are timezone-aware and serialized in ISO 8601 UTC.
- Topic keys remain `order_id` for order events and `service` for application
  logs.
- Do not change `.v1` topic names without the approved cross-track process.

### Application structure

- Keep executable applications in `apps/` and reusable code in `src/`.
- Keep business logic out of CLI modules, Kafka polling loops, and Docker
  entrypoint scripts.
- Separate configuration, Kafka clients, event models, repositories, business
  services, reports, and metrics.
- Avoid circular imports and duplicated Kafka or database setup.

## Dependency Rules

- Do not add a dependency unless the active phase requires it.
- Every new dependency must have a clear purpose and belong to the active
  phase.
- Verify Python 3.14.6 compatibility by installation and test execution.
- Do not silently lower the Python version.
- If Airflow or another tool is incompatible, report it and propose an isolated
  environment.
- Cloud dependencies must not become requirements for mandatory local tests.
- Put optional dependencies in a separate dependency group.
- Never commit credentials or secret configuration.

## Data Modeling Rules

- Separate source, staging, intermediate, and mart responsibilities.
- Staging performs rename, cast, and normalization; it does not define business
  metrics or cross-source joins.
- Marts do not read sources directly unless the active specification explicitly
  permits it.
- Every intermediate model and mart must document its grain.
- Use decimal/numeric types for money and never use floating-point output for
  formal monetary values.
- Do not aggregate different currencies together without an explicitly
  specified FX conversion.
- Every published mart requires an owner, contract, tests, description, and SLO
  metadata.
- Use descriptive model names and only existing source columns.
- Never invent a field merely to make SQL compile.

## Phase 8 Billing and Cloud Rules

### Phase 8A — Local-only mandatory work

- Runs completely locally.
- Requires no GCP credentials, credit card, or Billing Account.
- Creates no cloud resources.
- Cost data is limited to static validation or fixture-based simulation.
- Every simulated report must be clearly labeled.
- Do not create fake BigQuery job IDs.
- Do not describe simulated bytes as a real BigQuery measurement.
- Allowed evidence levels are `static_validation` and `simulated`.

### Phase 8B — Optional BigQuery Sandbox validation

- Is optional and does not require Billing.
- Re-check the official Google Cloud Sandbox limitations immediately before
  execution and record the source and check date.
- Do not assume Sandbox supports DML, `MERGE`, streaming, Data Transfer Service,
  or Cloud Composer.
- Only real Sandbox-returned evidence may be labeled `sandbox_observed`.
- Phase 8B not being completed does not block Phase 9 or Phase 10.

### Phase 8C — Optional deferred full cloud validation

- Defaults to `deferred` and requires a Billing-enabled project.
- Begin only after the user explicitly requests and approves Phase 8C.
- Before approval, do not enable Billing, create datasets, run BigQuery jobs,
  modify IAM, create paid resources, or execute cloud deployment.
- Before starting, present possible cost exposure, budget strategy, query byte
  limit, cleanup plan, region, credentials strategy, and rollback plan.
- Only real Billing-enabled BigQuery evidence may be labeled `cloud_observed`.

## Evidence Levels

Allowed evidence levels are:

- `static_validation`: parser, lint, metadata, or configuration validation only;
- `simulated`: fixture, mock, or local synthetic result;
- `sandbox_observed`: real response from BigQuery Sandbox;
- `cloud_observed`: real response from a Billing-enabled cloud environment;
- `not_available`: optional integration or artifact does not exist.

Rules:

- Never present simulated evidence as observed evidence.
- Do not replace missing values with zero.
- Do not fabricate job IDs, bytes processed, runtime, or cloud cost.
- Missing cloud artifacts must not fail the mandatory local workflow.
- Every acceptance report includes the command, exit code, status, evidence
  level, execution environment, relevant output, skipped reason, and timestamp.
- Evidence and report timestamps use timezone-aware UTC.

## Generated Code Verification

AI-generated code is not complete merely because files were created. Run the
checks required by the active phase; do not select only the easiest checks.

### General

- formatting;
- lint;
- type checking;
- unit tests;
- integration tests when applicable;
- documentation checks;
- secret review;
- existing Phase 1–5 tests.

### dbt

- `dbt deps`;
- `dbt parse`;
- `dbt compile`;
- affected `dbt build`;
- data tests and unit tests;
- contracts and descriptions;
- documentation generation.

### Phase 8A

- static SQL, partition, and cost policies;
- simulated-report labeling;
- Airflow DAG import;
- Airflow task unit tests.

### Phase 9

- metadata index validation and lineage tests;
- MCP input/output schema tests;
- timeout and secret-redaction tests;
- audit-log tests.

### Phase 10

- Skill smoke tests and deterministic helper tests;
- degraded-mode and evidence-classification tests;
- mutation-safety tests.

## Repository-local Codex Skills

Repository-local Skills belong in:

```text
.agents/skills/
```

This project uses Codex and must not add Claude Code-specific dependencies.
Planned Phase 10 Skills are:

```text
.agents/skills/dbt-scaffold/
.agents/skills/dbt-pr-review/
.agents/skills/incident-diagnosis/
```

Do not add these before their active phase. Each implemented Skill must have a
`SKILL.md` defining:

- invocation conditions;
- required inputs and context;
- allowed tools;
- prohibited actions;
- execution and validation steps;
- expected output;
- failure handling;
- completion criteria.

A Skill cannot override an explicit user instruction, `AGENTS.md`, or either
active specification.

Mandatory routing once the Skills exist:

- When implementing or generating a dbt model, use `dbt-scaffold`, read
  `DATA_PLATFORM_SPEC.md`, inspect real sources and columns, validate the model,
  and do not report completion until the required dbt checks pass.
- When reviewing dbt changes, use `dbt-pr-review` and inspect contracts,
  documentation, tests, lineage impact, incremental safety, and cost policies.
- When diagnosing a data incident, use `incident-diagnosis` with only allowed
  read-only MCP tools, distinguish facts, inferences, and unknowns, and report a
  degraded result when evidence is missing.

Skills must not independently commit, push, merge, deploy, rerun production,
reset Kafka offsets, or mutate schemas.

## MCP Safety

MCP tools must:

- be read-only by default;
- use explicit input and output schemas;
- return bounded output;
- enforce timeouts;
- use allowlisted artifact paths and prevent path traversal;
- redact secrets;
- produce audit logs;
- return degraded status when optional artifacts are missing.

MCP tools must not provide:

- arbitrary SQL;
- unrestricted shell execution;
- arbitrary filesystem reads or any filesystem write;
- production schema mutation;
- pipeline rerun;
- Kafka offset reset;
- PR merge;
- IAM mutation;
- cloud-resource creation.

If Phase 8B or Phase 8C is incomplete, corresponding BigQuery tools return
`not_available`. Optional cloud integrations must not become mandatory
failures. PostgreSQL metadata, quality, lineage, Kafka lag, and benchmark tools
must remain usable.

## Incident Diagnosis Safety

The first Agent version may only query, inspect, correlate, analyze, summarize,
recommend, generate remediation or backfill plans, and generate validation
commands for human review.

It must not:

- execute unrestricted SQL or arbitrary shell commands;
- delete or truncate tables;
- mutate schemas;
- rerun production pipelines;
- reset Kafka offsets or alter consumer groups;
- merge PRs or push commits;
- change IAM or create cloud resources;
- claim a root cause unsupported by evidence.

Agent output must distinguish confirmed facts, inferences, unknowns, rejected
hypotheses, confidence, and evidence references. Insufficient evidence requires
a degraded diagnosis.

## Coding Standards

- Use Python type hints.
- Public functions and classes have useful docstrings.
- Prefer small, focused functions; avoid functions longer than approximately 50
  lines unless justified.
- Do not use bare `except`, swallow exceptions, or discard exception context.
- Use structured logging instead of `print` in application code.
- Keep configuration in environment variables and configuration modules.
- Do not hard-code credentials, hostnames, ports, topics, or consumer groups.
- Handle SIGTERM and SIGINT in long-running consumers.
- Flush pending producer messages and aggregate buffers during graceful
  shutdown.
- Keep implementation readable enough to explain during an interview.

## Testing and Quality Commands

Use unit tests for isolated validation, aggregation, retry, DLQ, duplicate,
configuration, metadata, lineage, policy, and evidence-classification logic.
Use real Kafka and PostgreSQL containers for important integration behavior;
do not mock away all external interactions.

Kafka integration coverage must preserve:

- producer-to-Kafka delivery and consumption;
- valid event persistence;
- invalid event delivery to DLQ;
- duplicate idempotency;
- offset advancement after successful processing;
- no offset commit after database failure.

End-to-end Kafka coverage remains:

```text
start infrastructure
→ create topics
→ run migrations
→ start consumers
→ send mixed events
→ verify business rows and DLQ
→ verify duplicate handling
→ verify consumer lag returns toward zero
```

The repository must continue to support:

```bash
make lint
make typecheck
make test
```

When applicable, also run the active specification's exact completion gate,
including Docker Compose, migration, integration, demo, dbt, policy, MCP, or
Skill commands. Always report the exact command, exit code, meaningful output,
and failures or skipped reasons. Never claim a test passed without executing it.

## Documentation Rules

- Keep `README.md` oriented toward GitHub visitors and interviewers.
- Keep Phase 1–5 requirements in `SPEC.md` and Phase 6–10 requirements in
  `DATA_PLATFORM_SPEC.md`.
- Keep Codex working rules in `AGENTS.md`.
- Update architecture, reliability, modeling, quality, and phase documents when
  corresponding implementation decisions change.
- Add benchmark or cloud numbers only after they are measured.
- Record hardware and Docker environment for performance tests.
- Clearly distinguish local, simulated, Sandbox-observed, cloud-observed,
  production-recommended, optional, deferred, and not-implemented behavior.
- Do not describe planned or partially accepted functionality as complete.

## Security Rules

- Keep secrets in environment variables, local uncommitted configuration, or a
  CI secret store.
- Commit `.env.example`, never `.env`.
- Never commit service-account keys, API keys, tokens, private keys, passwords,
  or complete connection strings.
- Do not log credentials, sensitive payloads, complete environment dumps, or
  unnecessary identifiers.
- Local plaintext Kafka is acceptable only for this development MVP and is not
  production-ready.
- Data Platform tools should expose metadata and aggregates, not raw user rows.

## Git and Commit Rules

- Do not commit, push, or merge unless the user explicitly requests it.
- Before a requested commit, show modified files, untracked files, test results,
  and the proposed commit message.
- Do not add generated secrets, dbt `target/` artifacts, or local credentials to
  Git.
- Commit reports only when the active specification's evidence policy permits
  it.
- Never name or present simulated evidence as cloud evidence.
- Preserve unrelated user changes in a dirty worktree.

## Completion Report Format

Every completed phase report must use this structure:

```md
## Phase Completion Report

### Active Phase

### Specification Used

### Files Changed

### Dependencies Changed

### Commands Executed

| Command | Exit Code | Result | Evidence Level |
|---|---:|---|---|

### Acceptance Criteria

### Skipped Checks

### Known Limitations

### Backward Compatibility

### Security Review

### Next Legal Phase

### Commit Message Suggestion
```

`Next Legal Phase` is informational only and must not be started automatically.
Mark incomplete Phase 8B and Phase 8C as optional or deferred. Never describe a
partial pass as fully accepted.

## Definition of Completion

A phase is complete only when:

1. its required implementation exists;
2. its active specification's acceptance criteria are satisfied;
3. its relevant tests, lint, and type checks pass;
4. documentation reflects actual behavior and evidence;
5. backward compatibility and security reviews pass;
6. no later or optional phase was implemented unintentionally;
7. the completion report contains executed evidence.

If no phase is explicitly stated, stop after repository inspection and
planning. If a phase is completed, report and stop at its boundary.
