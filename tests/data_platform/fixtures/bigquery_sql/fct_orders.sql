select order_id, latest_order_state
from analytics.fct_orders
where order_id = 'fixture-order'
