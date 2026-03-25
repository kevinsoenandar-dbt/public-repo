{% macro view_schema_binding_update(schema_binding_behaviour) %}

{% if execute %}
    {% set materialized = config.get('materialized') %}

    {% if materialized | lower == 'view' %}
        {% if schema_binding_behaviour | lower in ('binding', 'evolution', 'compensation')%}
        alter view {{ this }} with schema {{ schema_binding_behaviour }}
        {% else %}
        {{ exceptions.raise_compiler_error("Invalid schema binding behaviour: " ~ schema_binding_behaviour ~ ". Allowed values are: binding, evolution, compensation") }}
        {% endif %}
    {% endif %}
{% endif %}
{% endmacro %}