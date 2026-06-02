{% macro normalize_sku(col) %}
    UPPER(TRIM(REPLACE({{ col }}, '-', '')))
{% endmacro %}
