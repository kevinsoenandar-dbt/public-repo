{#

    This macro is used to create a table copy of the selected resources in a dbt job command. This macro is meant to be used to circumvent the shallow / deep cloning 
    limitation on tables with RLS and column masking applied in Databricks.

    The macro works by:
    1. Accessing the selected_resources context to get the list of nodes to create table copies for.
    2. For each selected node, it will filter out non-model nodes and non-incremental models.
    3. For each incremental model node, it will check the existence of a defer_relation object
    4. If a defer_relation object is found, it will create a table copy of the node using the defer_relation object.
    5. If no defer_relation object is found, it will skip the node.

    To use the macro, add the following to the dbt_project.yml file:
    on-run-start:
      - "{{ create_table_copy(var('should_run', False)) }}"

    This will ensure that by default, the macro will NOT run. Then, in the CI job setting of the dbt platform,
    update the job command from `dbt clone --select state:modified+,config.materialized:incremental,state:old` to `dbt compile --select state:modified+,state:old --vars '{"should_run": True}'`
    
    Notes:
    1. The macro will handle the temporary schema creation as the schema creation is done _after_ the on-run-start hook is executed by default
    2. Do not add the `config.materialized:incremental` flag to the node selection in the job command as this will skip the macro entirely from running

#}

{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% do log("Hook is run at: " ~ run_started_at, info=True) %}

    {% if execute %}
        {% if should_run %}

            {% do _create_temp_pr_schema() %}

            {% for node in selected_resources %}
                {% set node_object = graph.nodes.values() 
                    | selectattr("resource_type", "equalto", "model") 
                    | selectattr("config.materialized", "equalto", "incremental")
                    | selectattr("unique_id", "equalto", node) | first %}
                {% if node_object | length > 0 %}
                    {% set deferral_node = node_object.defer_relation.relation_name %}
                    {% if deferral_node %}
                        {% do log("Found deferral node of: " ~ deferral_node, info=True) %}
                        {% do _execute_create_table_query(node_object.name, deferral_node) %}
                    {% else %}
                        {% do log("No deferral node found for: " ~ node_object.name ~ ", skipping table copy creation...", info=True) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}

    {% endif %}
{% endmacro %}

{% macro _create_temp_pr_schema() %}
    {% set create_temp_pr_schema_query %}
    create schema if not exists {{ target.database }}.{{ target.schema }}
    {% endset %}

    {% do run_query(create_temp_pr_schema_query) %}
{% endmacro%}

{% macro _execute_create_table_query(table_name, defer_relation) %}

    {% set create_table_query %}
    create table {{ target.schema }}.{{ table_name }} like {{ defer_relation }}
    {% endset %}

    {% do log("Executing create table query...", info=True) %}
    {% do run_query(create_table_query) %}
{% endmacro %}