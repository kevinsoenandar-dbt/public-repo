-- macros/drop_quoted_iceberg_table.sql
{#
    Drops a Glue-cataloged Iceberg table by its quoted-lowercase identity.

    Iceberg tables registered through Snowflake's Glue REST CLD live as
    quoted-lowercase objects (e.g. "DB"."schema_lower"."table_lower"),
    while dbt-snowflake's default relation cache uses uppercase folding.
    The mismatch causes "found an approximate match" compilation errors
    on the second and subsequent runs.

    Use as a pre_hook on Iceberg-materialised models so the existing
    object is removed before the materialisation does its get_relation
    check, avoiding the case-mismatch path.
#}
{% macro drop_quoted_iceberg_table(model) %}
    {% set sql_statement %}
        'DROP ICEBERG TABLE IF EXISTS dbt_ksoenandar_staging."'~ model.schema | lower ~ '"."' ~ model.identifier | lower  ~ '"'
    {% endset %}
    {{ sql_statement }}
    {# 'DROP ICEBERG TABLE IF EXISTS dbt_ksoenandar_staging."{{ model.schema | lower }}"."{{ model.alias | lower }}"' #}
{% endmacro %}