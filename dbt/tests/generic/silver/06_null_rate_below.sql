{% test null_rate_below(model, column_name, max_pct) %}

with stats as (
    select
        count(*)                                              as total_rows,
        count(*) filter (where {{ column_name }} is null)     as null_rows
    from {{ model }}
)
select
    total_rows,
    null_rows,
    round(100.0 * null_rows / total_rows, 2) as null_pct
from stats
where total_rows > 0
  and 100.0 * null_rows / total_rows > {{ max_pct }}

{% endtest %}
