{% test count_consistency(model) %}
select *
from {{ model }}
where success_count + client_error_count + server_error_count <> request_count
{% endtest %}
