{% macro generate_schema_name(custom_schema_name, node) -%}
    {% set node_meta = node.config.meta if node is not none and node.config.meta else {} %}
    {% if node_meta.get('blue_green', false) and var('blue_green_candidate', false) %}
        {{ var('blue_green_stage_schema') | trim }}
    {% else %}
        {{ generate_schema_name_for_env(custom_schema_name, node) }}
    {% endif %}
{%- endmacro %}
