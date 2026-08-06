select kafka_topic, kafka_partition, kafka_offset
from {{ ref('stg_order_events') }}
group by kafka_topic, kafka_partition, kafka_offset
having count(*) > 1
