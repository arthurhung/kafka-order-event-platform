select
    metric_minute::timestamptz as metric_minute,
    (metric_minute at time zone 'UTC')::date as metric_date,
    service::varchar(100) as service,
    endpoint::varchar(255) as endpoint,
    request_count::bigint as request_count,
    success_count::bigint as success_count,
    client_error_count::bigint as client_error_count,
    server_error_count::bigint as server_error_count,
    response_time_sum_ms::bigint as response_time_sum_ms,
    {{ safe_divide('response_time_sum_ms', 'request_count') }} as average_response_time_ms,
    max_response_time_ms::integer as max_response_time_ms,
    updated_at::timestamptz as updated_at
from {{ source('streaming_platform', 'log_metrics_minute') }}
