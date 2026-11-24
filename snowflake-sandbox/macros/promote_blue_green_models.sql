{% macro promote_blue_green_models(results) %}
    {% if execute
        and var('blue_green_candidate', false)
        and var('blue_green_promote', false)
        and target.name == var('blue_green_production_target', 'prod') %}
        {% set candidates = namespace(items=[]) %}
        {% for result in results %}
            {% set node_meta = result.node.config.meta if result.node.config.meta else {} %}
            {% if result.node.resource_type == 'model' and node_meta.get('blue_green', false) %}
                {% do candidates.items.append(result) %}
            {% endif %}
        {% endfor %}

        {% if candidates.items | length == 0 %}
            {% do log('Blue-green promotion skipped: no opted-in models ran.', info=True) %}
        {% else %}
            {% set gate = namespace(passed=true, reason='') %}
            {% for result in results %}
                {% set status = result.status | string | lower %}
                {% if status not in ['success', 'pass'] %}
                    {% set gate.passed = false %}
                    {% set gate.reason = 'At least one selected model or test did not pass.' %}
                {% endif %}
            {% endfor %}

            {% for candidate in candidates.items %}
                {% set candidate_tests = namespace(items=[]) %}
                {% for result in results %}
                    {% if result.node.resource_type == 'test'
                        and candidate.node.unique_id in result.node.depends_on.nodes %}
                        {% do candidate_tests.items.append(result) %}
                    {% endif %}
                {% endfor %}
                {% if candidate_tests.items | length == 0 %}
                    {% set gate.passed = false %}
                    {% set gate.reason = 'An opted-in model has no test results in this command.' %}
                {% endif %}
            {% endfor %}

            {% if not gate.passed %}
                {% do log('Blue-green promotion skipped: ' ~ gate.reason, info=True) %}
            {% else %}
                {% for candidate in candidates.items %}
                    {% set destination_database = candidate.node.config.database
                        if candidate.node.config.database is not none else target.database %}
                    {% set destination_schema = generate_schema_name_for_env(candidate.node.config.schema, candidate.node) %}
                    {% set source_relation = api.Relation.create(
                        database=candidate.node.database,
                        schema=candidate.node.schema,
                        identifier=candidate.node.alias
                    ) %}
                    {% set destination_relation = api.Relation.create(
                        database=destination_database,
                        schema=destination_schema,
                        identifier=candidate.node.alias
                    ) %}
                    {% if source_relation.render() == destination_relation.render() %}
                        {% do exceptions.raise_compiler_error('Blue-green source and production relations are identical for ' ~ candidate.node.name) %}
                    {% endif %}
                    {% do run_query('create or replace table ' ~ destination_relation ~ ' clone ' ~ source_relation ~ ' copy grants') %}
                    {% do log('Promoted ' ~ candidate.node.name ~ ' to ' ~ destination_relation, info=True) %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
