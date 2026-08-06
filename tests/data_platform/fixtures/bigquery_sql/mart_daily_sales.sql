select event_date, currency, channel, sum(paid_amount) as paid_amount
from analytics.mart_daily_sales
where event_date >= date '2026-08-01'
  and event_date < date '2026-09-01'
group by event_date, currency, channel
