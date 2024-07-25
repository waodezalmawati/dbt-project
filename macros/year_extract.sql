{% macro year_extract(date_column) %}
   EXTRACT(YEAR FROM {{ date_column }})

{% endmacro %}

