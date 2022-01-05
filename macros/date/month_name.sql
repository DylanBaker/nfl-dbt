{% macro month_name(date, short=True) -%}
  {{ return(adapter.dispatch('month_name')(date, short)) }}
{%- endmacro %}

{% macro default__month_name(date, short) -%}
{% set f = 'MON' if short else 'MONTH' %}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{% macro bigquery__month_name(date, short) -%}
{% set f = '%b' if short else '%B' %}
    format_date('{{ f }}', cast({{ date }} as date))
{%- endmacro %}
