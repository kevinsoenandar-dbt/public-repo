{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {% set node_meta = node.config.meta if node is not none and node.config.meta else {} %}
    {% if node_meta.get('blue_green', false) and var('blue_green_candidate', false) %}
        {{ var('blue_green_stage_database') | trim }}
    {% elif custom_database_name is none %}
        {{ target.database }}
    {% else %}
        {{ custom_database_name | trim }}
    {% endif %}
{%- endmacro %}
