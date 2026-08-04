# AGENTS.md

## Project Context

This repository implements a Kafka-based real-time event and application log processing platform.

The complete MVP requirements are defined in:

- `SPEC.md`

Before making any change, read `SPEC.md` and inspect the current repository state.

## Working Agreement

1. Implement only the phase explicitly requested by the user.
2. Do not begin the next phase until the current phase passes its acceptance criteria.
3. Before editing code:
   - summarize the requested phase;
   - list the files that will be created or modified;
   - identify unclear requirements, risks, and assumptions.
4. Prefer the simplest implementation that satisfies `SPEC.md`.
5. Do not silently change:
   - Kafka topic names;
   - partition counts;
   - consumer group names;
   - event schemas;
   - database schemas;
   - retry behavior;
   - offset commit behavior.
6. Update tests and documentation whenever behavior changes.
7. Do not create placeholder implementations that falsely appear complete.
8. Do not report success unless the related command or test was actually executed.
9. When requirements conflict, follow this priority:
   1. explicit user instruction;
   2. `SPEC.md`;
   3. this file;
   4. existing repository conventions.

## Local Python Environment

- Required Python version: `3.14.6`.
- The local interpreter is managed by pyenv.
- The project virtualenv is `kafka_streaming`, created from Python `3.14.6`.
- Current local interpreter path: `/Users/arthur/.pyenv/versions/kafka_streaming/bin/python`.
- Add and commit a `.python-version` file containing `kafka_streaming`.
- Set `requires-python = ">=3.14,<3.15"` in `pyproject.toml`.
- Prefer `python`, `python -m pip`, and project Makefile commands after `pyenv local kafka_streaming`; do not hard-code the user-specific absolute interpreter path in application code, Docker files, scripts, or shared configuration.
- Before implementation, verify the selected interpreter with:

```bash
python --version
pyenv version
python -c "import sys; print(sys.executable)"
```

- Dependency compatibility with Python 3.14 must be verified by actual installation and test execution. Do not downgrade Python or replace a dependency silently. Report the incompatibility first.

## Current MVP Scope

The MVP includes:

- Python 3.14.6, managed locally with pyenv
- Apache Kafka in single-node combined KRaft mode
- PostgreSQL
- Kafka UI
- Python event generator
- Order event consumer
- Application log consumer
- Pydantic validation
- Manual Kafka offset commits
- Idempotent database processing
- Dead Letter Queue
- Retry with bounded exponential backoff
- Structured JSON logging
- Benchmark and consumer lag scripts
- Unit, integration, and end-to-end tests
- Docker Compose
- Makefile
- README and technical documentation

## Out-of-Scope Technologies

Do not add the following unless the user explicitly requests them:

- Flink
- Spark
- Debezium
- Kafka Connect
- Schema Registry
- ClickHouse
- Doris
- Hologres
- Prometheus
- Grafana
- Kubernetes
- Terraform
- Multi-broker Kafka
- Cloud infrastructure
- Exactly-once claims across Kafka and PostgreSQL

These may appear in the roadmap, but they must not be introduced into the MVP implementation without explicit approval.

## Implementation Phases

Follow the phases defined in `SPEC.md`.

### Phase 1 — Project Skeleton

Expected focus:

- `.python-version` pinned to the Python 3.14.6 virtualenv `kafka_streaming`;
- `pyproject.toml` requiring Python `>=3.14,<3.15`;
- package structure;
- settings;
- structured logging;
- Docker Compose;
- Kafka KRaft;
- PostgreSQL;
- Kafka UI;
- Makefile;
- database migration;
- topic bootstrap.

Do not implement event generation or consumers during Phase 1.

### Phase 2 — Event Models and Generator

Expected focus:

- Pydantic event models;
- order and log event generation;
- configurable EPS and duration;
- invalid-event injection;
- duplicate-event injection;
- producer delivery handling.

### Phase 3 — Order Consumer

Expected focus:

- validation;
- manual offset commit;
- PostgreSQL transaction;
- idempotency;
- retry;
- DLQ handling;
- graceful shutdown.

### Phase 4 — Log Consumer

Expected focus:

- log validation;
- in-memory per-minute aggregation;
- periodic flush;
- PostgreSQL upsert;
- late-event updates;
- graceful shutdown.

### Phase 5 — Benchmark and Documentation

Expected focus:

- benchmark runner;
- consumer lag inspection;
- report generation;
- end-to-end demo;
- README;
- architecture and reliability documents;
- actual measured results only.

## Kafka Reliability Rules

These rules are mandatory:

1. Disable automatic offset commit.
2. Commit Kafka offsets only after downstream processing succeeds.
3. Store the idempotency record and business data in the same PostgreSQL transaction.
4. If PostgreSQL processing fails, do not commit the Kafka offset.
5. Permanent validation and decoding errors must be sent to the DLQ.
6. Commit the original message offset only after the DLQ message is successfully produced.
7. A poison message must not permanently block the partition.
8. All retries must have a maximum attempt count.
9. Retryable failures must use bounded exponential backoff.
10. Duplicate events must not create duplicate business records.
11. Do not claim exactly-once delivery.
12. Do not claim production readiness.
13. Do not treat `acks=all` on a single-node cluster as multi-replica durability.

## Database Rules

- Use SQLAlchemy 2.x style.
- Use Alembic for schema migrations.
- Use UTC and timezone-aware datetime values.
- Use `Decimal` for money.
- Do not use Python `float` for database monetary writes.
- Keep transaction boundaries explicit.
- Repository functions must not silently commit unless their contract clearly states it.
- Idempotency and business writes must share one transaction.
- Add indexes required by the query patterns defined in `SPEC.md`.

## Event and Schema Rules

- Use Pydantic 2.x models.
- Every event must include:
  - `event_id`;
  - `event_type`;
  - `event_version`;
  - `event_time`;
  - `source`;
  - `payload`.
- `event_id` must be globally unique for newly generated events.
- Duplicate injection must intentionally reuse an existing `event_id`.
- Event timestamps must be timezone-aware and serialized in ISO 8601.
- Topic keys must follow `SPEC.md`:
  - order events: `order_id`;
  - application logs: `service`.
- Do not change `.v1` topic names without explicit approval.

## Application Structure Rules

- Keep executable applications in `apps/`.
- Keep reusable code in `src/`.
- Keep business logic out of:
  - CLI modules;
  - Kafka polling loops;
  - Docker entrypoint scripts.
- Separate:
  - configuration;
  - Kafka clients;
  - event models;
  - database repositories;
  - business services;
  - reporting and metrics.
- Avoid circular imports.
- Do not duplicate Kafka or database setup across applications when shared code is appropriate.

## Coding Standards

- Use Python type hints.
- Public functions and classes must have useful docstrings.
- Prefer small, focused functions.
- Avoid functions longer than approximately 50 lines unless justified.
- Do not use bare `except`.
- Do not swallow exceptions.
- Preserve exception context when wrapping errors.
- Use structured logging instead of `print` in application code.
- Keep configuration in environment variables and configuration modules.
- Do not hard-code credentials, hostnames, ports, topic names, or consumer group names.
- Handle SIGTERM and SIGINT in long-running consumers.
- Flush pending producer messages and aggregate buffers during graceful shutdown.
- Keep the implementation readable enough to explain during an interview.

## Testing Requirements

### Unit Tests

Use unit tests for isolated logic such as:

- Pydantic validation;
- aggregation calculations;
- retry backoff;
- DLQ message construction;
- duplicate detection;
- configuration parsing.

### Integration Tests

Use real Kafka and PostgreSQL containers for important integration behavior.

Do not mock away all Kafka and database interactions.

At minimum, integration tests must verify:

- producer-to-Kafka delivery;
- Kafka consumption;
- valid event persistence;
- invalid event delivery to DLQ;
- duplicate event idempotency;
- offset advancement after successful processing;
- no offset commit after database failure.

### End-to-End Tests

The end-to-end flow must cover:

```text
start infrastructure
→ create topics
→ run migrations
→ start consumers
→ send mixed events
→ verify business rows
→ verify DLQ rows/messages
→ verify duplicate handling
→ verify consumer lag returns toward zero
```

## Quality Commands

After each implementation phase, run the relevant commands.

The final project should support:

```bash
make lint
make typecheck
make test
```

When applicable, also run:

```bash
docker compose config
make up
make topics
make migrate
make test-integration
make demo
```

Always report:

- the exact command executed;
- whether it succeeded;
- meaningful failures;
- any command that could not be executed and why.

Never claim a test passed without executing it.

## Documentation Rules

- Keep `README.md` oriented toward GitHub visitors and interviewers.
- Keep implementation requirements in `SPEC.md`.
- Keep agent working rules in `AGENTS.md`.
- Update architecture and reliability documents when implementation decisions change.
- Do not add benchmark numbers until they are measured.
- Record the hardware and Docker environment used for performance tests.
- Clearly distinguish:
  - local development behavior;
  - simulated failure behavior;
  - production recommendations.

## Security Rules

- Keep secrets in `.env`.
- Commit `.env.example`, not `.env`.
- Do not log passwords, tokens, connection strings, or sensitive payloads.
- Local plaintext Kafka is acceptable only for this development MVP.
- Document that local settings are not production-ready.

## Definition of Completion

A phase is complete only when:

1. its required code exists;
2. its acceptance criteria in `SPEC.md` are satisfied;
3. relevant tests pass;
4. lint and type checks pass where applicable;
5. documentation reflects the actual implementation;
6. no later phase was implemented unintentionally.

If the current phase is not explicitly stated, stop after repository inspection and planning. Do not guess which phase to implement.
