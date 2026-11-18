{% test is_true(model, column_name, allow_null=true) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is false
{% if not allow_null %}
   or {{ column_name }} is null
{% endif %}

{% endtest %}
