{% test conditional_not_null(model, column_name, condition) %}
select * from {{ model }} where ({{ condition }}) and {{ column_name }} is null
{% endtest %}
