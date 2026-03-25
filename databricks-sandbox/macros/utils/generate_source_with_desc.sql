{#
    This macro accepts the same arguments as the codegen generate source macro:
    Arguments

    schema_name (required): The schema name that contains your source data
    database_name (optional, default=target.database): The database that your source data is in. You must put in the actual database name, not the environment variable. 
                                                       If you want to use the environment variable, manually override the database name once the YML is generated.
    table_names (optional, default=none): A list of tables that you want to generate the source definitions for.
    generate_columns (optional, default=False): Whether you want to add the column names to your source definition.
    include_descriptions (optional, default=False): Whether you want to add description placeholders to your source definition.
    include_data_types (optional, default=True): Whether you want to add data types to your source columns definitions.
    table_pattern (optional, default='%'): A table prefix / postfix that you want to subselect from all available tables within a given schema.
    exclude (optional, default=''): A string you want to exclude from the selection criteria
    name (optional, default=schema_name): The name of your source
    include_database (optional, default=False): Whether you want to add the database to your source definition
    include_schema (optional, default=False): Whether you want to add the schema to your source definition
    case_sensitive_databases (optional, default=False): Whether you want database names to be in lowercase, or to match the case in the source table — not compatible with Redshift
    case_sensitive_schemas (optional, default=False): Whether you want schema names to be in lowercase, or to match the case in the source table — not compatible with Redshift
    case_sensitive_tables (optional, default=False): Whether you want table names to be in lowercase, or to match the case in the source table — not compatible with Redshift
    case_sensitive_cols (optional, default=False): Whether you want column names to be in lowercase, or to match the case in the source table

#}

{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', table_names=None, exclude='') %}

    {% if table_names %}
        {% set table_pattern = '%' %}
        {% set table_name_filter = [] %}
        {% for table in table_names %}
            {% do table_name_filter.append("'" ~ table ~ "'")%}
        {% endfor %}
        {% set table_name_filter = "(" ~ table_name_filter | join(',') ~ ")" %}
    {% endif %}
    
    {% set tables=get_tables_info_with_description(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        table_names=table_name_filter,
        exclude=exclude
    ) %}

    {% set table_list = [] %}

    {% for table in tables %}
        {% do table_list.append({table.identifier: table.metadata.description}) %}
    {% endfor %}

    {{ return(table_list) }}

{% endmacro %}

{% macro generate_source_with_desc(schema_name, database_name=target.database, generate_columns=False, include_descriptions=False, include_data_types=True, table_pattern='%', exclude='', name=schema_name, table_names=None, include_database=False, include_schema=False, case_sensitive_databases=False, case_sensitive_schemas=False, case_sensitive_tables=False, case_sensitive_cols=False) %}
    {{ return(adapter.dispatch('generate_source_with_desc')(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols)) }}
{% endmacro %}

{% macro default__generate_source_with_desc(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols) %}

{# 
    The user should really only pass in EITHER the list of table_names OR the table_pattern, and NOT both. 
    If both are passed in, the macro will log a warning and only use the list of table_names.
#}

{% if table_names is not none and table_pattern != '%' %}
    {% do log("⚠️ Both table_names and table_pattern parameters were passed in. This operation will proceed with only the table_names parameter.", True) %}
    {% set table_pattern = '%' %}
{% endif %}

{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{% if include_descriptions | lower == 'true' %}
    {% do sources_yaml.append('    description: ""' ) %}
{% endif %}

{% if database_name != target.database or include_database %}
{% do sources_yaml.append('    database: ' ~ (database_name if case_sensitive_databases else database_name | lower)) %}
{% endif %}

{% if schema_name != name or include_schema %}
{% do sources_yaml.append('    schema: ' ~ (schema_name if case_sensitive_schemas else schema_name | lower)) %}
{% endif %}

{% do sources_yaml.append('    tables:') %}

{% set tables=get_tables_in_schema(schema_name, database_name, table_pattern, table_names, exclude) %}

{% for relation in tables %}
    {% set table = (relation.keys()|list)[0] %}
    {% set description = (relation.values()|list)[0] %}
    {% do sources_yaml.append('      - name: ' ~ (table if case_sensitive_tables else table | lower) ) %}
    {% if include_descriptions|lower == "true" and description %}
        {% do sources_yaml.append('        description: |' ) %}
        {% do sources_yaml.append('            ' ~ description|replace('\n', '\n            ')) %}
    {% elif include_descriptions|lower == "true" and not description %}
        {% do sources_yaml.append('        description: ""' ) %}
    {% endif %}
    {% if generate_columns | lower == 'true' %}
    {% do sources_yaml.append('        columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=get_columns_info_with_description(table_relation) %}

        {% for column in columns %}
            {% do sources_yaml.append('          - name: ' ~ (column.name if case_sensitive_cols else column.name | lower)) %}
            {% if include_data_types %}
                {% do sources_yaml.append('            data_type: ' ~ codegen.data_type_format_source(column)) %}
            {% endif %}
            {% if include_descriptions and column.comment != 'None'%}
                {% do sources_yaml.append('            description: |' ) %}
                {% do sources_yaml.append('                ' ~ column.comment|replace('\n', '\n                ')) %}
            {% elif include_descriptions and column.comment == 'None' %}
                {% do sources_yaml.append('            description: ""' ) %}
            {% endif %}
            {% if column.databricks_tags %}
                {% do sources_yaml.append('            config:') %}
                {% do sources_yaml.append('              meta:') %}
                {% do sources_yaml.append('                databricks_tags:') %}
                {% for tag_name, tag_value in fromjson(column.databricks_tags).items() %}
                    {% do sources_yaml.append('                  - ' ~ tag_name ~ ": " ~ tag_value) %}
                {% endfor %}
            {% endif %}
        {% endfor %}
            {% do sources_yaml.append('') %}

    {% endif %}

{% endfor %}


{% if execute %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}