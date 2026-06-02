{% macro normalize_date(col) %}
    {% if target.type == 'duckdb' %}
        COALESCE(
            TRY_STRPTIME({{ col }}, '%m/%d/%Y'),
            TRY_STRPTIME({{ col }}, '%Y-%m-%d')
        )::DATE
    {% else %}
        COALESCE(
            TRY_TO_DATE({{ col }}, 'MM/DD/YYYY'),
            TRY_TO_DATE({{ col }}, 'YYYY-MM-DD')
        )
    {% endif %}
{% endmacro %}
