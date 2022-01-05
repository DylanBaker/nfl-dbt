{% macro day_name(date, short=True) -%}
  {{ return(adapter.dispatch('day_name')(date, short)) }}
{%- endmacro %}

{% macro default__day_name(date, short) -%}
{% set f = 'Dy' if short else 'Day' %}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{% macro bigquery__day_name(date, short) -%}
{% set f = '%a' if short else '%A' %}
    format_date('{{ f }}', cast({{ date }} as date))
{%- endmacro %}
