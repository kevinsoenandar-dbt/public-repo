{# /* {% macro edw_function() -%}{%- endmacro %} */ #}

{% macro dw_cdc_column_names() -%}
    {%- set cdc_column_names = ["cdc_seq_no", "infa_process_timestamp", "dtl__capxrestart1", "dtl__capxrestart2", "dtl__capxuow", "dtl__capxuser"
        , "dtl__capxtimestamp", "dtl__capxaction", "dtl__capxrowid"] -%}
    {%- do return(cdc_column_names) -%}
{%- endmacro %}

{% macro dw_reserved_column_names() -%}
    {%- set reserved_names = ["dw_cdc_action", "dw_ops_type", "dw_batch_id", "dw_process_ts", "dw_rec_del_ind"
        , "dw_strt_ts", "dw_end_ts", "dw_hash_key", "dw_session_id", "dw_hash_diff", "dw_dbt_invocation_id", "dw_airflow_dag_run_id","dw_sort_sequence"] -%}
    {%- do return(reserved_names) -%}
{%- endmacro %}

{% macro dw_get_column_in_relation(relation) -%}
    {%- call statement("relation_columns", fetch_result=True) -%}
        {#- /* the check is redundent as table and view are interchangeable in snowflake */ -#}
        describe {% if relation.is_view -%}view{%- else -%}table{%- endif %} {{ relation }}
    {%- endcall -%}
    {%- set metadata = load_result("relation_columns") -%}
    {%- set columns = [] -%}

    {%- for row in metadata["data"] -%}
        {%- set column = {
                "name": dw_value_to_string(row[0])
                , "type": dw_value_to_string(row[1])
                , "kind": dw_value_to_string(row[2])
                , "is_null": dw_value_to_string(row[3])
                , "default": dw_value_to_string(row[4])
                , "primary_key": dw_value_to_string(row[5])
                , "unique_key": dw_value_to_string(row[6])
                , "check": dw_value_to_string(row[7])
                , "expression": dw_value_to_string(row[8])
                , "comment": dw_value_to_string(row[9])
                , "policy_name": dw_value_to_string(row[10])
            } -%}
        {%- do columns.append(column) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}

{% macro dw_get_column_updates(source_relation, target_relation) -%}

    {% set reserved_columns = dw_reserved_column_names() %}

    {%- call statement("column_updates", fetch_result=True) -%}
        with source_metadata as (
            select 
                table_catalog, table_schema, table_name, column_name
                , udf_column_type(data_type, coalesce(character_maximum_length, numeric_precision_radix, datetime_precision), numeric_scale) as column_type 
                , (case when is_nullable = 'YES' then true else false end) as is_null
                , comment
            from {{ source_relation.database | upper }}.information_schema.columns 
            where table_schema = '{{ source_relation.schema | upper }}' 
                and table_name = '{{ source_relation.identifier | upper }}'
                and column_name not in ('{{reserved_columns | join ("', '") | upper  }}')
        )

        , target_metadata as (
            select 
                table_catalog, table_schema, table_name, column_name
                , udf_column_type(data_type, coalesce(character_maximum_length, numeric_precision_radix, datetime_precision), numeric_scale) as column_type 
                , (case when is_nullable = 'YES' then true else false end) as is_null
                , comment
            from {{ target_relation.database | upper }}.information_schema.columns 
            where table_schema = '{{ target_relation.schema | upper }}' 
                and table_name = '{{ target_relation.identifier | upper }}'
                and column_name not in ('{{reserved_columns | join ("', '") | upper  }}')
        )

        select * from (
        select
            ifnull(s.column_name, t.column_name) as name
            , (case 
                when t.column_name is null then 1 -- new
                when s.column_name is null and not t.is_null then 2 -- exclude - ensure target is nullable
                when t.column_type <> s.column_type or t.comment <> s.comment then 3 -- type and comment update
                else 0 -- no change
                end) as change
            , s.column_type as type
            , s.is_null
            , s.comment
            , t.column_type as target_type
            , (case when t.is_null <> s.is_null then true else false end) as change_type
            , (case when t.is_null <> s.is_null then true else false end) as change_is_null
            , (case when t.comment <> s.comment then true else false end) as change_comment
        from source_metadata s
        full outer join target_metadata t
            on s.column_name = t.column_name
        ) where change <> 0
    {%- endcall -%}
    {%- set metadata = load_result("column_updates") -%}
    {%- set columns = [] -%}

    {%- for row in metadata["data"] -%}
        {%- set column = {
                "name": row[0]
                , "change": row[1]
                , "type": row[2]
                , "is_null": row[3]
                , "comment": dw_value_to_string(row[4])
                , "target_type": dw_value_to_string(row[5])
                , "change_type": row[6]
                , "change_is_null": row[7]
                , "change_comment": row[8]
            } -%}
        {%- do columns.append(column) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}

{% macro dw_generate_column_alter(columns, target_relation) -%}
    {%- set add_column_string -%}
        {%- for col in columns if col.change == 1 -%}
            column {{ col.name }} {{ col.type }}
            {%- if not col.is_null %} not null{%- endif %}
            {%- if col.comment != '' %} comment {{ col.comment }}{%- endif -%}
            {{", " if not loop.last}}
        {%- endfor -%}
    {%- endset -%}

    {%- set is_changed = False -%}

    {%- set alter_column_string -%}
        {%- for col in columns if col.change == 2 -%}
            column {{ col.name }} drop not null{{", " if not loop.last}}
            {%- set is_changed = True -%}
        {%- endfor -%}
        {% if is_change -%}, {% endif -%}
        {%- for col in columns if col.change == 3 and col.change_type -%}
            column {{ col.name }} type {{ col.type }}{{", " if not loop.last}}
            {%- set is_changed = True -%}
        {%- endfor -%}
        {% if is_change -%}, {% endif -%}
        {%- for col in columns if col.change == 3 and col.change_comment -%}
            column {{ col.name }} comment {% if col.comment == '' -%}null{%- else -%} col.comment {%- endif %}{{", " if not loop.last}}
        {%- endfor -%}
    {%- endset -%}

    {%- set column_update_string -%}
        {%- if add_column_string != '' -%}
            alter table {{ target_relation }} 
            add {{ add_column_string }}
        {%- endif %}
        {%- if is_change -%}; {%- endif %}
        {% if alter_column_string != '' -%}
            alter table {{ target_relation }} 
            alter {{ alter_column_string }}
        {%- endif -%}
    {%- endset -%}

    {%- do return(column_update_string) -%}
{%- endmacro %}

{% macro dw_get_column_list(columns, exclude_names=[]) -%}    
    {%- set new_columns = [] -%}
    {%- for column in columns -%}
        {%- if column.name | lower not in exclude_names -%}
            {%- do new_columns.append(column) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- do return(new_columns) -%}
{%- endmacro %}

{% macro dw_get_common_column_list(source_columns, target_columns, exclude_names=[]) -%}    
    {%- set common_columns = [] -%}
    {%- for column in target_columns -%}
        {#- /* loop through mapped list is popping out the item. so at the end of the loop, the list become none. */ -#}
        {%- set source_column_names = source_columns | map(attribute='name') -%}
        {%- if column.name in source_column_names and column.name | lower not in exclude_names -%}
            {%- do common_columns.append(column) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- do return(common_columns) -%}
{%- endmacro %}

{% macro dw_generate_column_create(columns) -%}
    {%- set create_column_string -%}
        {% for col in columns -%}
            {{ col.name }} {{ col.type }}
            {%- if col.is_null == 'N' %} not null{%- endif %}
            {%- if col.comment != '' %} comment '{{ col.comment }}'{%- endif %}{{", " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(create_column_string) -%}
{%- endmacro %}

{% macro dw_generate_column_compare(columns, source_alias="s", target_alias="t") -%}
    {%- set compare_column_string -%}
        {% for col in columns -%}
            ({{ source_alias }}.{{ col.name }} <> {{ target_alias }}.{{ col.name }} or ({{ source_alias }}.{{ col.name }} is null and {{ target_alias }}.{{ col.name }} is not null) or ({{ source_alias }}.{{ col.name }} is not null and {{ target_alias }}.{{ col.name }} is null))
            {{"or " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(compare_column_string) -%}
{%- endmacro %}

{% macro dw_generate_column_check(columns, source_alias="s", target_alias="t") -%}
    {%- set check_column_string -%}
        {% for col in columns -%}
            ({{ source_alias }}.{{ col.name }} = {{ target_alias }}.{{ col.name }} or ({{ source_alias }}.{{ col.name }} is null and {{ target_alias }}.{{ col.name }} is null)
            {{"and " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(check_column_string) -%}
{%- endmacro %}

{% macro dw_generate_column_null(columns, source_alias=none) -%}
    {%- set null_column_string -%}
        {% for col in columns -%}
            ({{ source_alias }}.{{ col.name }} is null {{"and " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(null_column_string) -%}
{%- endmacro %}

{% macro dw_generate_column_assignment(columns, source_alias="s") -%}
    {%- set assignment_column_string -%}
        {% for col in columns -%}
            {{ col.name }} = {{ source_alias }}.{{ col.name }}
            {{", " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(assignment_column_string) -%}
{%- endmacro %}

{% macro dw_generate_column_select(columns, source_alias=none) -%}
    {%- set select_column_string -%}
        {% for col in columns -%}
            {%- if source_alias is not none and source_alias is string -%}{{ source_alias }}.{%- endif -%}
            {{ col.name }}{{", " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {%- do return(select_column_string) -%}
{%- endmacro %}

{% macro dw_generate_masking_policy_set(target_columns, masking_policy) -%}
    {%- if masking_policy is none or masking_policy is not defined -%}
        {%- set masking_policy = {} -%}
    {%- endif -%}

    {%- set masking_columns = [] -%}
    {%- for column in target_columns -%}
        {%- set column_name = column.name | lower -%} 
        
        {%- if column_name in masking_policy -%}
            {%- do masking_columns.append("column " ~ column.name ~ " set masking policy " ~ masking_policy[column_name]) -%}
        {%- elif column_name not in masking_policy and column.policy_name != '' -%}
            {%- do masking_columns.append("column " ~ column.name ~ " unset masking policy") -%}
        {%- endif -%}
    {%- endfor -%}

    {%- do return(masking_columns) -%}
{%- endmacro %}

{% macro dw_update_tags(target_relation, tags) %}
    -- update table tags:
    {% set queries = [] %}
    {% set predefined_tags = ["FEATURE", "FEATURE_SET", "FEATURE_ENTITY","FEATURE_REFRESH_FREQUENCY","FEATURE_CLASSIFICATION"] %}
    {% for tag in tags %}
        {% set tag_parts = tag.split(':') %}
        {% if tag_parts[0] in predefined_tags %}
            {% set tag_query %}
                ALTER TABLE {{ target_relation }} SET TAG {{this.database}}.common.{{ tag_parts[0] }} = '{{ tag_parts[1] }}';
            {% endset %}
            {% do queries.append(tag_query) %}
        {% endif %}
    {% endfor %}

    {% if queries | length > 0 %}
        {% set full_query %}
            {{ queries | join(' ') }}
        {% endset %}
        {% do log('full_query: ' ~ full_query, info=True) %}
        {%- do return(full_query) -%}
    {% else %}
        {%- do return('') -%}
    {% endif %}
    
{% endmacro %}

{% macro dw_update_column_properties(target_relation, column_tags) %}
    {% set queries = [] %}
    -- loop thru column tags items:
    {% for column_name, column_properties in column_tags.items() %}
        {% set comment = column_properties.description %}
        {% set tags = column_properties.tags %}

        -- update column tags:
        {% set predefined_tags = ["FEATURE", "FEATURE_SET", "FEATURE_ENTITY","FEATURE_REFRESH_FREQUENCY","FEATURE_CLASSIFICATION"] %}
        {% for tag in tags %}
            {% set tag_parts = tag.split(':') %}
            {% if tag_parts[0] in predefined_tags %}
            {% set tag_query %}
                ALTER TABLE {{ target_relation }} MODIFY COLUMN {{ column_name }} SET TAG {{this.database}}.{{this.schema}}.{{ tag_parts[0] }} = '{{ tag_parts[1] }}';
            {% endset %}
            {% do queries.append(tag_query) %}
            {% endif %}
        {% endfor %}

        -- update column comment:
        {% if comment %}
            {% set comment_query %}
                ALTER TABLE {{ target_relation }} MODIFY COLUMN {{ column_name }} COMMENT '{{ comment }}';
            {% endset %}
            {% do queries.append(comment_query) %}
        {% endif %}
    {% endfor %}

    {% if queries | length > 0 %}
        {% set full_query %}
            {{ queries | join(' ') }}
        {% endset %}
        {% do log('full_query: ' ~ full_query, info=True) %}
        {%- do return(full_query) -%}
    {% else %}
        {%- do return('') -%}
    {% endif %}
{% endmacro %}