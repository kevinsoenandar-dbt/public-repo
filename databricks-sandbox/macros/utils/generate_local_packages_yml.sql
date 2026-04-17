{% macro get_local_package_mappings() %}
    {% do return({
        'dev_1': 'abc, def, ghi',
        'dev_2': 'abc, jkl',
        'dev_3': 'def, jkl, mno'
    }) %}
{% endmacro %}

{% macro generate_local_packages_yml() %}
    {% set selected_environment = env_var('DBT_DEVELOPER_ENVIRONMENT') %}
    {% set mappings = get_local_package_mappings() %}

    {% if selected_environment not in mappings %}
        {% do exceptions.raise_compiler_error(
            "No package mapping found for environment '" ~ selected_environment ~ "'."
        ) %}
    {% endif %}

    {% set package_paths = [] %}
    {% for package_path in mappings[selected_environment].split(',') %}
        {% do package_paths.append(package_path | trim) %}
    {% endfor %}

    {% set yaml_output %}
packages:
{%- for package_path in package_paths %}
  - local: {{ package_path }}
{%- endfor -%}
    {% endset %}

    {{ print(yaml_output | trim) }}
    {% do return(yaml_output | trim) %}
{% endmacro %}

