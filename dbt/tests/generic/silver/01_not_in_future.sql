{% test not_in_future(model, column_name, tolerance_days=0) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} > (current_timestamp at time zone 'utc')
                          + make_interval(days => {{ tolerance_days }})

{% endtest %}
