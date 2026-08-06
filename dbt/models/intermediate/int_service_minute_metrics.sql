select
    *,
    {{ safe_divide('success_count', 'request_count') }} as success_rate,
    {{ safe_divide('client_error_count', 'request_count') }} as client_error_rate,
    {{ safe_divide('server_error_count', 'request_count') }} as server_error_rate,
    {{ safe_divide('client_error_count + server_error_count', 'request_count') }} as error_rate
from {{ ref('stg_log_metrics_minute') }}
