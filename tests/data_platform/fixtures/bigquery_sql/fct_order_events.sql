select event_id, order_id, event_type, event_date
from analytics.fct_order_events
where event_date >= date '2026-08-01'
  and event_date < date '2026-09-01'
