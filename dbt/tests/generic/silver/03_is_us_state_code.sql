{% test is_us_state_code(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} !~ '^[A-Z]{2}$'

{% endtest %}
