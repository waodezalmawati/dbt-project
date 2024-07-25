{% macro month_extract(date_column) %}
   EXTRACT(MONTH FROM {{ date_column }})

{% endmacro %}

