# Phase 6 Modeling Semantics

## Order lifecycle

`valid_orders` is one row per persisted event, not one row per order. The deterministic
sequence uses Kafka topic, partition, and offset within each `order_id`, with `event_id` only
as a deterministic tie-breaker for invalid duplicate coordinates. Data tests fail if a
coordinate is duplicated or an order crosses partitions. `event_time` remains business time
and may arrive late, so it does not select the latest state.

`latest_order_state` is the latest known Kafka-stream state. It is not a financial final
settlement state. A failed payment can be followed by a paid event; multiple paid or failed
events are counted as payment attempts because no stable payment ID is stored. Paid and
cancelled can both be true historically, while the latest state follows the last stream event.
The source cannot prove authorization, capture, refund, chargeback, final settlement, failure
reason, cancellation reason, or payment method semantics.

Currency is retained on every monetary product. Cross-event consistency tests reject silent
currency changes within an order. `mart_daily_sales` groups by date, currency, and channel;
it performs no FX conversion and its paid amount is not accounting revenue.

## Service health

The source grain is one minute, service, and endpoint. Endpoint rates use guarded division.
Service totals sum counts and response-time numerators before division:

```text
weighted_average_response_time_ms = sum(response_time_sum_ms) / sum(request_count)
```

The implementation never averages endpoint averages. When request count is zero, success
rate, error rate, and average response time are null. Tests reject negative counters, duplicate
composite grain, and rows where success, client-error, and server-error counts do not equal
request count.

## Contract limitation

All four published marts enforce dbt contracts. Monetary columns are declared as PostgreSQL
`numeric` in contract metadata because the dbt-postgres contract parser in the verified
version does not safely render a comma-bearing `numeric(18,2)` type declaration. Model SQL
still casts source monetary values to `numeric(18,2)`. This preserves decimal semantics, but
precision and scale are verified through SQL behavior rather than the contract type string.

## Phase 7 scaffold boundary

The scaffold is a draft generator, not a schema inference system. It normalizes model names by
layer and supplies metadata and contract structure, but it does not select sources, upstream
models, columns, business keys, monetary semantics, or transformations. Missing decisions stay
as `BLOCKING_TODO` markers and convention validation fails until a developer replaces them with
verified definitions. Existing Phase 6 model SQL and contract semantics remain unchanged.

## Phase 8A physical-design declarations

BigQuery fields in mart metadata describe future physical design only. Daily sales and service
health plan bounded partition overwrite to preserve their existing aggregate grains. Order events
plan an `event_id` merge while retaining Kafka-coordinate lifecycle order. `fct_orders` is exempt
from partitioning because a mutable latest-state row has no stable calendar partition. These
declarations do not change PostgreSQL materialization, contract columns, money, currency grouping,
or lifecycle semantics and have not been executed by BigQuery.
