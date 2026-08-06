select order_id
from {{ ref('stg_order_events') }}
group by order_id
having count(distinct user_id) > 1
