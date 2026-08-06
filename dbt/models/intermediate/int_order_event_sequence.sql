with ordered as (
    select
        *,
        row_number() over (
            partition by order_id
            order by kafka_topic, kafka_partition, kafka_offset, event_id
        )::bigint as event_sequence_number
    from {{ ref('stg_order_events') }}
),

sequenced as (
    select
        *,
        lag(event_type) over order_stream as previous_event_type,
        lead(event_type) over order_stream as next_event_type,
        lag(event_time) over order_stream as previous_event_time,
        first_value(event_time) over order_stream as first_event_time,
        last_value(event_time) over order_stream as latest_event_time
    from ordered
    window order_stream as (
        partition by order_id
        order by event_sequence_number
        rows between unbounded preceding and unbounded following
    )
)

select
    *,
    extract(epoch from event_time - previous_event_time)::numeric as seconds_since_previous_event
from sequenced
