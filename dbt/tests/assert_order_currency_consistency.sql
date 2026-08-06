select order_id
from {{ ref('stg_order_events') }}
where currency is not null
group by order_id
having count(distinct currency) > 1
