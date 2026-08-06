select metric_date, service, sum(request_count) as request_count
from analytics.mart_service_health
where metric_date >= date '2026-08-01'
  and metric_date < date '2026-08-08'
group by metric_date, service
