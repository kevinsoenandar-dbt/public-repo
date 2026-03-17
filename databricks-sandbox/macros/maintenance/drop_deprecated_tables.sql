{# drop_deprecated_tables

The purpose of this macro is to remove any stale tables/views that no longer have a corresponding model in dbt project.

Args:
    - dry_run: boolean string ('True' or 'False'); Jinja does not play nicely with boolean values -- dry run flag. When dry_run is true, the cleanup commands are printed to stdout rather than executed. This is true by default
    - catalog: string; The catalog to scan for deprecated tables. This defaults to the target catalog listed in the connection for the environment
    - environment: string; The environment we are running the macro in. This defaults to 'dev'; 
                    the logic currently only checks dev vs everything else. Can be extended to account for different environments in the future
                    in development environment, you do not need to pass the environment argument. This is only required in staging and production environments.

Example 1 - dry run of environment database (useful for staging and production environments, where there is only one catalog to check and no additional schemas to check)
    dbt run-operation drop_deprecated_tables

Example 2 - actual run of environment database (useful for staging and production environments, where there is only one catalog to check and no additional schemas to check)
    dbt run-operation drop_deprecated_tables --args '{"dry_run": "False"}'

Example 3 - dry run of specific database
    dbt run-operation drop_deprecated_tables --args '{"catalog": "pciprod_dbt-core-dev"}'

#}

{% macro drop_deprecated_tables(dry_run='True', catalog=target.database, environment='dev') %}

    {% if execute %}

        {% set current_models=[] %}
        -- only check for schemas that are present in the dbt project
        {% set schemas_to_check=[] %}

        {% if additional_schemas | length > 0 %}
            {% set schemas_to_check = schemas_to_check + additional_schemas %}
        {% endif %}

        {% for node in graph.nodes.values()
            | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
            -- we want to append the alias if it exists (the current behaviour is alias = name if no alias is specified) and the name if no alias exists
            -- alias should take precedence as this is what is materialised in the data platform.
            {% set current_model = node.alias if node.alias else node.name %}
            -- node.schema already includes the target.schema prefix if environment is dev
            {% set relation_name = node.database ~ '.' ~ node.schema ~ '.' ~ current_model %}
            {% do current_models.append(relation_name) %}
        {% endfor %}

    {% endif %}
    
    {% set cleanup_query %}
        
        WITH 
        models_to_drop AS (
            SELECT
                CASE
                    -- we start with just the basic relation types. We can add more as needed.
                    -- no official documentation on dropping streaming tables but dbt-databricks adapter (https://github.com/databricks/dbt-databricks/blob/main/dbt/include/databricks/macros/relations/streaming_table/drop.sql#L6) uses this command
                    WHEN table_type in ('MANAGED', 'STREAMING_TABLE') then 'TABLE'
                    WHEN table_type = 'VIEW' then 'VIEW'
                    WHEN table_type = 'MATERIALIZED_VIEW' then 'MATERIALIZED VIEW'
                END as relation_type,
                CONCAT_WS('.', '`' || table_catalog || '`', '`' || table_schema || '`', '`' || table_name || '`') as relation_name
            FROM
                `{{ catalog }}`.`information_schema`.`tables`
            WHERE 
                UPPER(TABLE_SCHEMA) != 'INFORMATION_SCHEMA'

                -- in development environment, we only want to check for schemas that start with the target.schema prefix, otherwise we will get not authorised errors.
                {% if environment.lower() == 'dev' %}
                    and UPPER(TABLE_SCHEMA) ilike '{{ target.schema }}%'
                {% endif %}
                -- uniformly apply all full relation names to uppercase for comparison to avoid unintended false negatives
                and UPPER(CONCAT_WS('.', table_catalog, table_schema, table_name)) not in
                    (
                        {%- for model in current_models -%}
                            '{{ model.upper() }}'
                            {%- if not loop.last -%},{% endif %}
                        {%- endfor -%}
                    )
        )

        SELECT
            'drop ' || relation_type || ' ' || relation_name || ';' as drop_commands,
            relation_name
        FROM
            models_to_drop
        -- intentionally exclude unhandled table_types, including 'external table`
        WHERE relation_type IS NOT NULL

    {% endset %}

    {% set cleanup_query_run_results = run_query(cleanup_query) %}
    {% set commands_to_drop = cleanup_query_run_results.columns[0].values() %}
    {% set tables_to_drop = cleanup_query_run_results.columns[1].values() %}
    {% set drop_commands = zip(commands_to_drop, tables_to_drop) %}
    
    {% if tables_to_drop | length > 0 %}
        {% do log("="*200, info=True) %}
        {% do log("Cleaning up deprecated tables in " ~ catalog ~ " for schemas: " ~ schemas_to_check | join(', '), info=True) %}
        {% do log("="*200, info=True) %}
        {% for drop_command, relation_name in drop_commands %}

            {% if dry_run|lower == 'false' %}
                {% do log('Dropping deprecated table ' ~ relation_name, True) %}
                {% do run_query(drop_command) %}
                {% do log('Deprecated table ' ~ relation_name ~ ' dropped.', True) %}
            {% else %}
                {% do log('Dry run mode: would drop deprecated table ' ~ relation_name ~ ' with command: ' ~ drop_command, True) %}
            {% endif %}

        {% endfor %}
        
    {% else %}
        {% do log('No relations to clean.', True) %}

    {% endif %}

{%- endmacro -%}