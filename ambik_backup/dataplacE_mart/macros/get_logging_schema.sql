-- your_dbt_project/macros/get_audit_schema.sql
{% macro get_audit_schema() %}

   {{ return('metadata_logging') }}

{% endmacro %}