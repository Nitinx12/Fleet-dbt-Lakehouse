{#
    Helper macros for the gold marts.

    mart_watermark   high-water mark of the fact layer already folded into THIS mart.
                     Compared against fact.gold_loaded_at (not silver_loaded_at), because marts read
                     gold facts, and gold_loaded_at is refreshed whenever a fact row is rebuilt.
    mart_key         surrogate key over the mart grain (single column, so delete+insert has one unique_key).
    mart_hash        fingerprint of the denormalised dimension attributes stored on a mart row. Used to
                     detect SCD1 overwrites in a dimension and re-compute only the affected mart rows.
    mart_ratio       safe division: ratio of sums, NULL when the denominator is 0 / NULL.
    mart_month_key   YYYYMM integer from a dim_date row; -1 when the fact points at the Unknown date.
#}

{% macro mart_watermark(column='source_gold_loaded_at') -%}
    (SELECT COALESCE(MAX({{ column }}), '1900-01-01'::TIMESTAMPTZ) FROM {{ this }})
{%- endmacro %}


{% macro mart_key(exprs) -%}
    MD5(CONCAT_WS('|'{% for e in exprs %}, ({{ e }})::TEXT{% endfor %}))
{%- endmacro %}


{% macro mart_hash(exprs) -%}
    MD5(CONCAT_WS('|'{% for e in exprs %}, COALESCE(({{ e }})::TEXT, '~'){% endfor %}))
{%- endmacro %}


{% macro mart_ratio(num, den, digits=4) -%}
    ROUND(({{ num }})::NUMERIC / NULLIF(({{ den }})::NUMERIC, 0), {{ digits }})
{%- endmacro %}


{% macro mart_month_key(alias) -%}
    COALESCE({{ alias }}.year_number * 100 + {{ alias }}.month_number, -1)
{%- endmacro %}
