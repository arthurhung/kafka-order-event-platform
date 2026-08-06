# Phase 6 Data Quality

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
