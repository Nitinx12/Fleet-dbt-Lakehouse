{% test max_scale(model, column_name, max_digits=2) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and abs(({{ column_name }})::numeric - round(({{ column_name }})::numeric, {{ max_digits }})) > 0

{% endtest %}
