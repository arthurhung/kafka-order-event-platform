{% test single_value_per_group(model, column_name, group_by) %}
select {{ group_by }}
from {{ model }}
group by {{ group_by }}
having count(distinct {{ column_name }}) filter (where {{ column_name }} is not null) > 1
{% endtest %}
