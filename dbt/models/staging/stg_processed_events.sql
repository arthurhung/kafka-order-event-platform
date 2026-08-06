select
    consumer_group::varchar(100) as consumer_group,
    event_id::uuid as event_id,
    topic::varchar(255) as kafka_topic,
    partition_id::integer as kafka_partition,
    offset_id::bigint as kafka_offset,
    processed_at::timestamptz as processed_at
from {{ source('streaming_platform', 'processed_events') }}
