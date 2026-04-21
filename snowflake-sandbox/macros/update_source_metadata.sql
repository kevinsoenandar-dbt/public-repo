{#
    Self-contained macro to generate dbt source YAML (codegen-style).
    When include_descriptions=true, queries a metadata table per source table (Option B)
    to fetch column descriptions.

    Metadata table naming: {metadata_table_prefix}{source_table_name}{metadata_table_suffix}
    Example: metadata_raw_orders (for source table raw_orders)

    Metadata table schema: | column_name | column_description |
#}

{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', exclude='') %}

    {% set tables = dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        exclude=exclude
    ) %}

    {% set table_list = tables | map(attribute='identifier') %}

    {{ return(table_list | sort) }}

{% endmacro %}


{% macro data_type_format_source(column) %}
    {{ return(column.data_type | lower) }}
{% endmacro %}


{% macro get_column_descriptions_from_metadata(
    table_name,
    metadata_database,
    metadata_schema,
    metadata_table_prefix,
    metadata_table_suffix,
    metadata_column_name_column,
    metadata_description_column
) %}
    {% set metadata_table_name = (metadata_table_prefix | default('')) ~ table_name ~ (metadata_table_suffix | default('')) %}
    {% set metadata_relation = api.Relation.create(
        database=metadata_database,
        schema=metadata_schema,
        identifier=metadata_table_name
    ) %}


    {% set metadata_exists = dbt_utils.get_relations_by_pattern(
        schema_pattern=metadata_schema,
        database=metadata_database,
        table_pattern=metadata_table_name
    )%}

    {% set column_descriptions = {} %}
    {% if metadata_exists %}
        {% set sql %}
            SELECT {{ metadata_column_name_column }}, {{ metadata_description_column }}
            FROM {{ metadata_relation }}
        {% endset %}

        {% set result = run_query(sql) %}

        {% if result and result.rows %}
            {% for row in result.rows %}
                {% do column_descriptions.update({row[0]: row[1]}) %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ return(column_descriptions) }}
{% endmacro %}


{% macro update_source_metadata(
    schema_name,
    database_name=target.database,
    generate_columns=false,
    include_descriptions=false,
    include_data_types=false,
    table_pattern='%',
    exclude='',
    name=schema_name,
    table_names=none,
    include_database=false,
    include_schema=false,
    case_sensitive_databases=false,
    case_sensitive_schemas=false,
    case_sensitive_tables=false,
    case_sensitive_cols=false,
    metadata_table_prefix="",
    metadata_table_suffix='_metadata',
    metadata_schema=none,
    metadata_database=none,
    metadata_column_name_column='column_name',
    metadata_description_column='column_description'
) %}

{% set metadata_schema = metadata_schema if metadata_schema is not none else schema_name %}
{% set metadata_database = metadata_database if metadata_database is not none else database_name %}

{% set sources_yaml = [] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{% if include_descriptions %}
    {% do sources_yaml.append('    description: ""') %}
{% endif %}

{% if database_name != target.database or include_database %}
{% do sources_yaml.append('    database: ' ~ (database_name if case_sensitive_databases else database_name | lower)) %}
{% endif %}

{% if schema_name != name or include_schema %}
{% do sources_yaml.append('    schema: ' ~ (schema_name if case_sensitive_schemas else schema_name | lower)) %}
{% endif %}

{% do sources_yaml.append('    tables:') %}

{% if table_names is none %}
{% set tables = get_tables_in_schema(schema_name, database_name, table_pattern, exclude) %}
{% else %}
{% set tables = table_names %}
{% endif %}

{% for table in tables %}
    {% do sources_yaml.append('      - name: ' ~ (table if case_sensitive_tables else table | lower)) %}
    {% if include_descriptions %}
        {% do sources_yaml.append('        description: ""') %}
    {% endif %}
    {% if generate_columns %}
    {% do sources_yaml.append('        columns:') %}

        {% set table_relation = api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns = adapter.get_columns_in_relation(table_relation) %}

        {% if include_descriptions %}
            {% if metadata_schema is none or metadata_database is none %}
                {% do log("Metadata schema or database name is not set. Please set the metadata_schema, and metadata_database parameters.", True) %}
            {% else %}
            {% set column_descriptions = get_column_descriptions_from_metadata(
                table,
                metadata_database,
                metadata_schema,
                metadata_table_prefix,
                metadata_table_suffix,
                metadata_column_name_column,
                metadata_description_column
            ) %}
            {% endif %}
        {% endif %}

        {% for column in columns %}
            {% do sources_yaml.append('          - name: ' ~ (column.name if case_sensitive_cols else column.name | lower)) %}
            {% if include_data_types %}
                {% do sources_yaml.append('            data_type: ' ~ data_type_format_source(column)) %}
            {% endif %}
            {% if include_descriptions %}
                {% set desc = column_descriptions.get(column.name, column_descriptions.get(column.name | lower, '')) | default('') %}
                {% if desc %}
                    {% do sources_yaml.append('            description: "' ~ desc | replace('"', '\\"') ~ '"') %}
                {% else %}
                    {% do sources_yaml.append('            description: ""') %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% do sources_yaml.append('') %}

    {% endif %}

{% endfor %}

{% if execute %}

    {% set joined = sources_yaml | join('\n') %}
    {{ print(joined) }}
    {{ return(joined) }}

{% endif %}

{% endmacro %}
