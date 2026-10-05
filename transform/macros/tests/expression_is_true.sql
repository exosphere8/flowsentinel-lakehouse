{#- Fails for every row where the expression is false. A small, dependency-free version of
    dbt_utils.expression_is_true. Works as a model-level or a column-level test; at column
    level the expression is still written out in full. -#}
{% test expression_is_true(model, expression, column_name=none, where=none) %}
select *
from {{ model }}
where not ({{ expression }})
{%- if where %} and ({{ where }}){% endif %}
{% endtest %}
