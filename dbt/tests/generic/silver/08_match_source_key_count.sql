{% test matches_source_key_count(model, column_name, source_model, source_column=none, tolerance_pct=0) %}

{%- set src_col = source_column if source_column else column_name -%}

with silver as (
    select count(distinct {{ column_name }}) as key_count from {{ model }}
),
bronze as (
    select count(distinct {{ src_col }}) as key_count from {{ source_model }}
)
select
    silver.key_count as silver_keys,
    bronze.key_count as bronze_keys
from silver
cross join bronze
where abs(silver.key_count - bronze.key_count) > bronze.key_count * {{ tolerance_pct }} / 100.0

{% endtest %}
