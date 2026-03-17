{#
    This is a macro inspired by the dbt_utils.get_relations_by_pattern (https://github.com/dbt-labs/dbt-utils/blob/main/macros/sql/get_relations_by_pattern.sql) macro.
    This macro adds the ability to pull in the table description (aka comment) from the information_schema.tables table.
#}

{% macro get_tables_info_with_description(schema_pattern, table_pattern, table_names=None, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_tables_info_with_description')
        (schema_pattern, table_pattern, table_names, exclude, database)) }}
{% endmacro %}

{% macro default__get_tables_info_with_description(schema_pattern, table_pattern, table_names,exclude='', database=target.database) %}

    {% call statement('get_tables_info_with_description', fetch_result=True) %}
        select distinct
            table_schema as {{ adapter.quote('table_schema') }},
            table_name as {{ adapter.quote('table_name') }},
            {{ dbt_utils.get_table_types_sql() }},
            comment as {{ adapter.quote('table_description') }}
        from {{ database }}.information_schema.tables
        where table_schema ilike '{{ schema_pattern }}'
        and table_name ilike '{{ table_pattern }}'
        {% if table_names %}
        and table_name in {{ table_names }}
        {% endif %}
        and table_name not ilike '{{ exclude }}'

        order by table_name
    
    {% endcall %}

    {% set table_list = load_result('get_tables_info_with_description') %}

    {% if table_list and table_list['table'] %}
        {% set tbl_relations = [] %}
        {% for row in table_list['table'] %}

            -- We create Databricks relations here with the additional metadata to hold the description
            {% set rel = api.Relation.create(
                database=database,
                schema=row['table_schema'],
                identifier=row['table_name'],
                metadata={'description': row['table_description'] | trim('\n')}
            ) %}

            {% do tbl_relations.append(rel) %}
        {% endfor %}

        {{ return(tbl_relations) }}

    {% else %}
        {{ return([]) }}
    
    {% endif%}

{% endmacro %}