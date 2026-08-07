# Phase 6 Data Quality

Phase 10 preserves deterministic quality gates. Scaffold cannot invent a column, PR review cannot
override a blocking contract finding, and incident facts require evidence IDs. Partial, stale, or
missing required artifacts produce degraded diagnosis; unavailable optional cloud evidence remains
distinct from failure. Four fixed scenarios cover pipeline freshness, Kafka lag, quality failure,
and insufficient evidence.

The project applies source, staging, intermediate, mart, singular, and dbt unit tests.
Coverage includes primary/composite uniqueness, required values, accepted event types,
conditional payload nullability, positive quantity, non-negative money and metrics, status
count consistency, Kafka coordinate uniqueness, per-order partition/user/currency consistency,
relationships where the Kafka Core guarantees them, and currency-safe mart grain.

The ten dbt unit scenarios cover created-to-paid, failed-then-paid, cancellation,
paid-then-cancelled, inconsistent currency visibility, Kafka order taking precedence over
event time, a deterministic duplicate-coordinate tie-breaker, multi-currency daily sales,
weighted service latency, and zero-request guarded division. Real source duplicate coordinates
and currency inconsistency still fail data tests; the tie-breaker only prevents nondeterministic
query output while surfacing invalid input.

Source freshness uses the latest persisted timestamps and is meaningful only after loading a
fresh fixture or processing current events. It proves local source recency, not upstream SLA or
production availability. `make data-platform-fixtures` publishes deterministic, run-scoped
events through the real Kafka consumers. Reusing a run ID skips already persisted event IDs,
does not reset offsets, does not truncate tables, and writes an ignored machine-readable local
summary. Benchmark reports remain valid because benchmark measurement isolates its own Kafka
coordinates rather than relying on whole-table counts.

Integration tests snapshot source identities before and after dbt build, verify that no dbt
relations appear in `public`, require all four mart tables in the isolated analytics schema,
and verify an identical fixture run publishes no additional events.

Phase 7 adds three deterministic quality layers. Convention validation reads a freshly parsed
manifest and checks model placement, naming, grain, ownership, SLO metadata, contracts, column
documentation, direct source use, wildcard policy, and incomplete scaffold markers. Existing
intermediate wildcards are warnings; staging and published-mart wildcards are blocking.

Contract comparison reads previous and current manifests. Column/model removal, same-type rename
candidates, incompatible type changes, nullable-to-required changes, grain/business-key changes,
contract removal, and required-test removal are blocking. Owner, SLO, materialization,
incremental-key, and metric-description changes require manual review. The checker explicitly
does not claim that manifest comparison can prove business-semantic equivalence.

Slim CI evidence records the base/current Git identities, state mode, selected models, command
exit codes, bounded sanitized output, and report paths. A missing base produces a named full-build
fallback rather than an empty or falsely successful state selection.

Phase 8A adds separate compatibility, partition/cluster, SQL, cost, provider, policy-diff, and
orchestration gates. Manifest and SQL-policy evidence is `static_validation`; fixed byte estimates
are `simulated`. Missing estimates remain null. Unsupported cloud providers return `not_available`,
and any blocking result prevents a passed aggregate summary. Existing intermediate wildcard
warnings and the Phase 7 contract/Slim CI report semantics remain unchanged.

Phase 9 validates artifact object structure, normalized Pydantic schemas, cross-file lineage nodes and
edges, deterministic sorting, MCP input/output envelopes, bounded traversal, timeout, redaction, and
audit records. A present malformed mandatory artifact is blocking. A missing non-manifest input is
explicitly degraded rather than silently treated as current or successful. Cost evidence selection
never falls back across simulated, Sandbox, or Cloud classifications.
