{{ config(contract={'enforced': true}) }}

select
    event_id,
    order_id,
    event_type,
    user_id,
    product_id,
    quantity,
    amount,
    currency,
    channel,
    event_time,
    event_date,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    persisted_at,
    event_sequence_number,
    previous_event_type,
    next_event_type,
    previous_event_time,
    seconds_since_previous_event,
    first_event_time,
    latest_event_time
from {{ ref('int_order_event_sequence') }}
