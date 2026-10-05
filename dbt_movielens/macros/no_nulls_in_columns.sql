{% macro no_nulls_in_columns(model) %}
    select * from {{ model }} where
    {% for column in adapter.get_columns_in_relation(model) %}
        {{ column.name }} is null or 
    {% endfor %}
    FALSE
{% endmacro %}