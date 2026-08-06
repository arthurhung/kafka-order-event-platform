{{ config(contract={'enforced': true}) }}

select
    metric_minute,
    metric_date,
    service,
    sum(request_count)::bigint as request_count,
    sum(success_count)::bigint as success_count,
    sum(client_error_count)::bigint as client_error_count,
    sum(server_error_count)::bigint as server_error_count,
    sum(client_error_count + server_error_count)::bigint as error_count,
    {{ safe_divide('sum(success_count)', 'sum(request_count)') }} as success_rate,
    {{ safe_divide('sum(client_error_count + server_error_count)', 'sum(request_count)') }}
        as error_rate,
    {{ safe_divide('sum(response_time_sum_ms)', 'sum(request_count)') }}
        as weighted_average_response_time_ms,
    max(max_response_time_ms)::integer as max_response_time_ms,
    count(distinct endpoint)::bigint as endpoint_count
from {{ ref('int_service_minute_metrics') }}
group by metric_minute, metric_date, service
