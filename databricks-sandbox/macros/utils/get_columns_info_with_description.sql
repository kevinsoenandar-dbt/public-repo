{% macro get_columns_info_with_description(relation) %}
    {{ return(adapter.dispatch('get_columns_info_with_description')
        (relation)) }}
{% endmacro %}

{% macro default__get_columns_info_with_description(relation) %}

    {% call statement('get_columns_info_with_description', fetch_result=True) %}

    with col_tags as (
    select
        table_name,
        column_name,
        parse_json(to_json(map_from_entries(collect_list(struct(tag_name, tag_value))))) AS databricks_tags
    
    from {{ relation.database }}.information_schema.column_tags

    group by catalog_name, schema_name, table_name, column_name
    )

    select
    columns.column_name as {{ adapter.quote('column_name') }},
    columns.full_data_type as {{ adapter.quote('column_data_type') }},
    columns.comment as {{ adapter.quote('column_description') }},
    columns.is_nullable as {{ adapter.quote('column_is_nullable') }},
    col_tags.databricks_tags as {{ adapter.quote('databricks_tags') }}

    from {{ relation.database }}.information_schema.columns

    left join col_tags
        on columns.table_name = col_tags.table_name
        and columns.column_name = col_tags.column_name

    where table_schema = '{{ relation.schema }}'
        and columns.table_name = '{{ relation.identifier }}'

    {% endcall %}

    {% set column_list = load_result('get_columns_info_with_description') %}

    {% if column_list and column_list['table'] %}
        {% set column_details = [] %}

        {% for row in column_list['table'] %}
            {% set col = api.Column.create(
                name=row['column_name'],
                label_or_dtype=row['column_data_type'],
            ) %}
            {% set col = col.enrich(model_column={
                'name':row['column_name'],
                'data_type':row['column_data_type'],
                'description':row['column_description'] | trim('\n'),
                'databricks_tags':row['databricks_tags']
            }, not_null=row['column_is_nullable']) %}
            {% do column_details.append(col) %}
        {% endfor %}

        {{ return(column_details) }}

    {% else %}
        {{ return([]) }}
    {% endif %}
    
{% endmacro %}