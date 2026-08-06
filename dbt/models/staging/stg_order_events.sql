select
    event_id::uuid as event_id,
    order_id::varchar(64) as order_id,
    event_type::varchar(50) as event_type,
    user_id::varchar(64) as user_id,
    product_id::varchar(64) as product_id,
    quantity::integer as quantity,
    amount::numeric(18, 2) as amount,
    currency::varchar(3) as currency,
    channel::varchar(20) as channel,
    event_time::timestamptz as event_time,
    (event_time at time zone 'UTC')::date as event_date,
    kafka_topic::varchar(255) as kafka_topic,
    kafka_partition::integer as kafka_partition,
    kafka_offset::bigint as kafka_offset,
    created_at::timestamptz as persisted_at
from {{ source('streaming_platform', 'valid_orders') }}
