# Phase 9 Metadata Index and Data Discovery MCP

Phase 9 turns fixed, machine-readable repository artifacts into a deterministic metadata index and
a local, read-only MCP server. It does not query source rows, execute SQL or shell commands, change
Kafka offsets, rerun pipelines, mutate schemas, or expose a public listener. Phase 8B and 8C remain
unstarted; their evidence is returned as `not_available`, never replaced by a fixture claim.

## Architecture and data flow

```text
dbt manifest/catalog/run_results/sources ─┐
Phase 7 quality + contract reports       ├─ allowlisted readers + JSON validation
Phase 8A policy/cost reports             ┤                │
consumer-lag + benchmark reports         ┘                v
                                      normalized assets + evidence identities
                                                   │
                         metadata-index.json + lineage-graph.json + index-summary.json
                                                   │
                                             read-only service
                                                   │
                                        local STDIO MCP adapter
                                                   │
                              Codex tools + sanitized JSONL audit log
```

The normalized asset grain is one record per dbt model or source. It records identity, layer,
description/grain, owner/domain/product metadata where declared, columns, materialization, contract,
latest quality/freshness status, and direct lineage. It stores report identities and classifications,
not report payloads or raw business rows. Logical output is stable: assets, edges, evidence identities,
and warnings are sorted, while `generated_at` comes from the manifest rather than wall-clock time.
Absolute source paths are not emitted.

## Inputs, degraded behavior, and evidence

Fixed dbt inputs are `dbt/target/manifest.json`, `catalog.json`, `run_results.json`, and
`sources.json`. Fixed report families cover Phase 7 quality/contract evidence, Phase 8A reports,
`reports/consumer-lag.json`, and committed `reports/runs/benchmark-*.json` evidence. Readers enforce
the repository root, fixed names/patterns, object-shaped JSON, and an 8 MiB per-file ceiling.

A missing manifest prevents index construction and exits 2. Other missing inputs produce a valid
`degraded` index naming each missing artifact. A malformed present required dbt artifact is rejected;
an invalid optional report is omitted with a warning. Missing Phase 8B/8C or cloud Airflow reports do
not prevent startup. The cost tool obeys the requested evidence level:

- `best_available` / `simulated` may return a Phase 8A fixture estimate labeled `simulated`.
- `sandbox_observed` returns `not_available` until genuine Phase 8B evidence exists.
- `cloud_observed` returns `not_available` until genuine Phase 8C evidence exists.
- Missing byte measurements stay `null`; simulated reports never contain an observed job ID.

Every MCP response uses `status`, `data`, `evidence`, `evidence_level`, `warnings`, and a UTC
`generated_at`. Allowed tool evidence levels are `static_validation`, `simulated`,
`sandbox_observed`, `cloud_observed`, and `not_available`. Indexing a local benchmark is itself
`static_validation`; its original report classification is retained separately as
`source_evidence_level=local_execution`.

## Tools and safety boundary

The server exposes `search_data_assets`, `get_model_schema`, `get_model_owner`, `get_lineage`,
`get_upstream_lineage`, `get_downstream_impact`, `get_quality_status`,
`get_recent_pipeline_failures`, `get_consumer_lag`, and `get_cost_estimate`. Pydantic schemas reject
extra fields, SQL-shaped search text, invalid evidence levels, excessive result limits, and lineage
depth above eight. Traversal is cycle-protected and capped at 200 nodes. Results are redacted and
limited to 256 KiB. Calls have a configurable timeout (five seconds by default).

The JSONL audit at `reports/metadata/mcp-audit.jsonl` records request ID, UTC timestamp, tool,
sanitized arguments, status, duration, result size, evidence count, and sanitized error category.
Keys resembling credentials and bearer tokens or connection strings are replaced with
`[REDACTED]`. The audit never records environment dumps. The audit write is server-internal; no MCP
file-write tool exists.

## Local commands and exit codes

Generate dbt artifacts and any reports you want indexed first, then run:

```bash
make metadata-index
make metadata-validate
make mcp-smoke
make test-mcp
make validate-phase9
```

`make mcp-server` is the long-running STDIO process. `metadata-index` exits 0 for schema-valid
complete or explicitly degraded output and 2 for a missing/malformed manifest or another invalid
present mandatory artifact. `metadata-validate` exits 0 for internally consistent complete or
degraded output and 1 for missing, malformed, or conflicting generated files. Smoke/tests exit 0 on
pass and non-zero on protocol or assertion failures. Reusing identical inputs rewrites the same
logical index; audit timestamps and request IDs are intentionally per call.

`make phase9-ci` also writes `metadata-validation-report.json`, `mcp-smoke-report.json`,
`security-report.json`, and `phase9-ci-summary.json`. The summary explicitly records
`mcp_transport=stdio`, `mcp_adapter=restricted_local_adapter`, official SDK runtime/install as
unavailable/not installed, no public listener, no cloud execution, no Airflow runtime, and false
values for GCP credential access, Billing use, and cloud-resource creation.

## Codex local MCP setup

The current official Codex manual says local clients support STDIO servers and share MCP settings.
For a trusted repository, either add the server through the Codex MCP settings UI or place an entry
like this in project-scoped `.codex/config.toml` (do not commit a user-specific absolute path):

```toml
[mcp_servers.retail_data_platform]
command = "python"
args = ["scripts/data_platform/run_mcp_server.py"]
cwd = "/absolute/path/to/kafka-order-event-platform"
startup_timeout_sec = 10
tool_timeout_sec = 10
enabled_tools = [
  "search_data_assets",
  "get_model_schema",
  "get_model_owner",
  "get_lineage",
  "get_upstream_lineage",
  "get_downstream_impact",
  "get_quality_status",
  "get_recent_pipeline_failures",
  "get_consumer_lag",
  "get_cost_estimate",
]
```

Build the index before restarting Codex. The equivalent CLI shape is
`codex mcp add retail_data_platform -- python scripts/data_platform/run_mcp_server.py`, run from the
repository root. Confirm with `codex mcp list` or `/mcp`.

## CI

The `phase9` GitHub Actions job requires both `phase7` and `phase8a`. It uses Python 3.14.6,
PostgreSQL, the local Kafka fixture path, fresh dbt build/freshness/docs artifacts, a machine-readable
lag report, local Phase 7/8A evidence, and `make phase9-ci`. It has no GCP credentials, Billing,
BigQuery adapter, Airflow runtime, cloud resource, or public MCP listener. `if: always()` copies only
the eight allowlisted diagnostics into `reports/metadata-ci/<run-id>/` and uploads that run-specific
directory; profiles, `.env`, credentials, dbt cache/target, logs, and unrelated reports are excluded.

## Dependency decision and limitations

External constraint review performed `2026-08-07T10:42:31+08:00`:

- Codex source: [official MCP configuration manual](https://developers.openai.com/codex/mcp/).
  Relevant behavior: project/global `config.toml`, STDIO command/args/cwd, startup/tool timeouts, and
  tool allowlists.
- SDK source: [official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) and
  [SDK installation guide](https://py.sdk.modelcontextprotocol.io/installation/). Relevant version:
  `mcp==2.0.0`, whose metadata declares Python 3.14 support.
- Actual compatibility check: `python -m pip install 'mcp==2.0.0'` failed on the verified macOS x86_64
  Python 3.14.6 environment because the transitive `cryptography` source build could not locate
  OpenSSL/pkg-config. Python was not downgraded and system packages were not added.

The selected implementation is therefore a narrow dependency-free STDIO MCP protocol adapter over
Pydantic models, covering initialization, ping, tool listing, and tool calls. It is smoke-tested over
a real child process. It deliberately has no HTTP transport or authentication surface. The adapter
must be retested against Codex when the host protocol changes; adopting the official SDK later
requires a separately compatible Python environment or available binary wheels.

Phase 9 is metadata discovery, not production observability. A report older than 24 hours is flagged
stale, history is unavailable unless an indexed history artifact exists, and missing lag never
triggers a live Kafka fallback. Phase 10 Skills and incident reasoning consume this layer without
expanding its read-only tool surface; see `phase-10.md`.

## Acceptance boundary

Acceptance requires deterministic index/lineage output, all ten read-only tools, schema and contract
validation, bounded traversal/result/timeout behavior, redaction/audit tests, honest unavailable cloud
evidence, local STDIO smoke, Phase 1–8 regression, and the Phase 9 completion gate. It does not accept
Phase 8B, Phase 8C, any real BigQuery/Airflow execution, Phase 10, or production readiness.
