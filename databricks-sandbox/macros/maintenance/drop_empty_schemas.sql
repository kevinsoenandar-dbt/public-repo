{# drop_empty_schemas

macros:
  - name: drop_empty_schemas
    description: 'This macro drops all schemas in the target database that are empty, except `PUBLIC` and `INFORMATION_SCHEMA`'
    example: dbt run-operation drop_empty_schemas
    args:
        - dry_run: boolean string ('True' or 'False'); Jinja does not play nicely with boolean values -- dry run flag. When dry_run is true, the cleanup commands are printed to stdout rather than executed. This is true by default
        - catalog: string; The catalog to scan for empty schemas. This defaults to the target catalog listed in the connection for the environment
        - environment: string; The environment we are running the macro in. This defaults to 'dev'; 
                    the logic currently only checks dev vs everything else. Can be extended to account for different environments in the future
                    in development environment, you do not need to pass the environment argument. This is only required in staging and production environments.

#}

{% macro drop_empty_schemas(dry_run='True', catalog=target.database, environment='dev') %}

  {% set catalog = "`" ~ catalog ~ "`" %}
  {% set cleanup_query %}

      WITH 
      
      ALL_SCHEMAS AS (
        SELECT
          CONCAT_WS('.', '`' || CATALOG_NAME || '`', '`' || SCHEMA_NAME || '`') AS SCHEMA_NAME
        FROM 
          {{ catalog }}.`information_schema`.`schemata`
        WHERE 
          SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          {% if environment.lower() == 'dev' %}
            and SCHEMA_NAME ilike '{{ target.schema }}%'
          {% endif %}
      ),

      NON_EMPTY_SCHEMAS AS (
        SELECT
          DISTINCT CONCAT_WS('.', '`' || TABLE_CATALOG || '`', '`' || TABLE_SCHEMA || '`') AS SCHEMA_NAME
        FROM 
          {{ catalog }}.`information_schema`.`tables`
        WHERE 
          TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          {% if environment.lower() == 'dev' %}
            and TABLE_SCHEMA ilike '{{ target.schema }}%'
          {% endif %}
      ),

      EMPTY_SCHEMAS AS (
        SELECT * FROM ALL_SCHEMAS
        MINUS
        SELECT * FROM NON_EMPTY_SCHEMAS
      )

      SELECT 
        'DROP SCHEMA ' || SCHEMA_NAME || ';' as DROP_COMMANDS,
        SCHEMA_NAME
      FROM 
        EMPTY_SCHEMAS

  {% endset %}

    
  {% set query_results = run_query(cleanup_query) %}
  {% set drop_commands = query_results.columns[0].values() %}
  {% set schemas_to_drop = query_results.columns[1].values() %}
  {% set drop_commands = zip(drop_commands, schemas_to_drop) %}

  {% if schemas_to_drop | length > 0 %}
    {% do log("="*200, info=True) %}
    {% do log("Cleaning up empty schemas in " ~ catalog, info=True) %}
    {% do log("="*200, info=True) %}
    {% for drop_command, schema_name in drop_commands %}
        {% if dry_run|lower == 'false' %}
            {% do log('Dropping empty schema ' ~ schema_name, True) %}
            {% do run_query(drop_command) %}
            {% do log('Empty schema ' ~ schema_name ~ ' dropped.', True) %}
        {% else %}
            {% do log('Dry run mode: would drop empty schema ' ~ schema_name ~ ' with command: ' ~ drop_command, True) %}
        {% endif %}
    {% endfor %}
  {% else %}
    {% do log('No schemas to clean.', True) %}
  {% endif %}
  
{% endmacro %}