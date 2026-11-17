{% macro clean_boolean(column_name) -%}
    case
        when lower(btrim({{ column_name }}::text)) in ('true', 't', '1', 'y', 'yes') then True
        when lower(btrim({{ column_name }}::text)) in ('false', 'f', '0', 'n', 'no') then False
    end
{%- endmacro %}
