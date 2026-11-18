{% test within_range(model, column_name, min_value=none, max_value=none) %}

{% if min_value is none and max_value is none %}
    {{ exceptions.raise_compiler_error('within_range needs min_value or max_value') }}
{% endif %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
{% if min_value is not none %}
  and {{ column_name }} < {{ min_value }}
{% endif %}
{% if max_value is not none %}
  and {{ column_name }} > {{ max_value }}
{% endif %}

{% endtest %}
