{% macro clean_string(column) -%}
    nullif(trim({{ column }}), '')
{%- endmacro %}

{% macro clean_date(column) -%}
    case
        when {{ column }} is null or trim({{ column }}::text) = '' then null
        else {{ column }}::date
    end
{%- endmacro %}

{% macro clean_timestamp(column) -%}
    case
        when {{ column }} is null or trim({{ column }}::text) = '' then null
        else {{ column }}::timestamp
    end
{%- endmacro %}


