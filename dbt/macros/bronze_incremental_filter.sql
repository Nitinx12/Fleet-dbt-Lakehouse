{% macro bronze_incremental_filter(column='_ingested_at') -%}
    {% if is_incremental() %}
    where {{ column }} >= (
        select coalesce(max({{ column }}), '1900-01-01'::timestamp)
               - make_interval(hours => {{ var('silver_lookback_hours', 24) }})
        from {{ this }}
    )
    {% endif %}
{%- endmacro %}
