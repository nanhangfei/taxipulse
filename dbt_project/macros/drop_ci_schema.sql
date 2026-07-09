{% macro drop_ci_schema() %}
  {% do run_query("drop schema if exists " ~ target.database ~ "." ~ target.schema ~ " cascade") %}
{% endmacro %}
