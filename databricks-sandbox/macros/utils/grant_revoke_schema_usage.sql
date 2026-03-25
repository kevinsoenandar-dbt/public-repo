-- macros/operations/grant_revoke_schema_usage.sql
{% macro grant_revoke_schema_usage() %}
{#-
  Dynamically grants USAGE on schemas based on roles that have SELECT
  privileges on objects within that schema.
  Fully quoted for Databricks, supports special characters in catalog, schema, and role names.

  This macro will also introspect roles with USAGE privilege on schemas and revoke USAGE on schemas for 
  roles that are not listed in the graphs.
-#}

{% set schema_grants = {} %}
{% set commands_to_run = [] %}

{% if execute %}
  {% set model_nodes = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | list %}
  {% for node in model_nodes %}
    {# Get the grants config for this node #}
    {% set grants = node.config.get('grants') %}
    {% set select_roles = grants['select'] if grants else [] %}

    {# Fully-qualified schema with backticks for catalog and schema #}
    {% set database_schema = adapter.quote(node.database) ~ "." ~ adapter.quote(node.schema) %}

    {# Merge roles safely, remove duplicates #}
    {% if database_schema in schema_grants %}
      {% set current_roles = schema_grants[database_schema] | list %}
      {% set schema_grants = schema_grants.update({
          database_schema: (current_roles + select_roles) | unique | list
      }) %}
    {% else %}
      {% set schema_grants = schema_grants.update({
          database_schema: select_roles | list
      }) %}
    {% endif %}
  {% endfor %}

  {% for schema, roles in schema_grants.items() %}

    {# Introspect all schemas to see which roles have USAGE privilege on the schema, 
    and revoke USAGE on schemas for roles that are not listed in the graphs.
    #}

    {% set query = "SHOW GRANTS ON SCHEMA " ~ schema ~ ";" %}
    {% set results = run_query(query) %}
    {% set roles_to_check = results | list | selectattr('ActionType', 'equalto', 'USE SCHEMA') | list %}
    {% set roles_to_check = roles_to_check | map(attribute='Principal') | list %}
    
    {# Loop through the roles to check and add revoke statements #}
    {% for revoke_role in roles_to_check %}
        {% if revoke_role not in roles %}
            {% do log("Revoking usage on schema " ~ schema ~ " from " ~ revoke_role, info=True) %}
            {% do run_query("REVOKE USAGE ON SCHEMA " ~ schema ~ " FROM " ~ adapter.quote(revoke_role)) %}
        {% endif %}
    {% endfor %}

    {% for grant_role in roles %}
        {% if grant_role not in roles_to_check %}
          {% do log("Granting usage on schema " ~ schema ~ " to " ~ grant_role, info=True) %}
          {% do run_query("GRANT USAGE ON SCHEMA " ~ schema ~ " TO " ~ adapter.quote(grant_role)) %}
        {% endif %}
    {% endfor %}

  {% endfor %}
{% endif %}

{% endmacro %}