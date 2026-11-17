{% test is_clean_text(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (   btrim({{ column_name }}) = ''
       or {{ column_name }} <> btrim({{ column_name }})
       or {{ column_name }} ~ '\s{2,}')

{% endtest %}
