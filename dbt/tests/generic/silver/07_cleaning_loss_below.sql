{% test cleaning_loss_below(model, column_name, source_model, source_column=none, max_increase_pct=5) %}

{%- set src_col = source_column if source_column else column_name -%}

with silver as (
    select 100.0 * count(*) filter (where {{ column_name }} is null)
                   / nullif(count(*), 0) as null_pct
    from {{ model }}
),
bronze as (
    select 100.0 * count(*) filter (where nullif(btrim({{ src_col }}::text), '') is null)
                   / nullif(count(*), 0) as null_pct
    from {{ source_model }}
)
select
    round(bronze.null_pct, 2)                   as bronze_null_pct,
    round(silver.null_pct, 2)                   as silver_null_pct,
    round(silver.null_pct - bronze.null_pct, 2) as increase_pct
from silver
cross join bronze
where silver.null_pct - bronze.null_pct > {{ max_increase_pct }}

{% endtest %}
