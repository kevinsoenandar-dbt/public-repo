{% macro dw_value_to_string(value) -%}
    {#- /* this essentially convert none to '' */ -#}
    {%- if value is not none -%} {{ value }} {%- endif -%}
{%- endmacro %}

{#- /* Normalise config.meta to a dict regardless of dbt version.
       dbt-fusion returns meta as a Python dict; dbt-core 1.9+ may return it as a
       JSON string. This helper handles both so materializations are portable. */ -#}
{% macro dw_get_meta(config) -%}
    {%- set raw = config.get("meta", {}) -%}
    {%- if raw is mapping -%}
        {%- do return(raw) -%}
    {%- elif raw is string and raw | length > 0 -%}
        {%- do return(modules.json.loads(raw)) -%}
    {%- else -%}
        {%- do return({}) -%}
    {%- endif -%}
{%- endmacro %}

{% macro dw_get_list(value_list) -%}
    {%- if value_list is not none and value_list is string -%}
        {%- set value_list = [value_list] -%}
    {%- endif -%}
    {%- do return(value_list) -%}
{%- endmacro %}

{% macro dw_list_to_string(value_list) -%}
    {%- if value_list is not none and value_list is string -%}
        {%- set value_list = [value_list] -%}
    {%- endif -%}

    {%- if value_list is not none -%}
        {%- set list_string = value_list | join(", ") -%}
    {%- else -%}
        {%- set list_string = none -%}
    {%- endif -%}

    {%- do return(list_string) -%}
{%- endmacro %}

{% macro dw_make_temp_relation(relation, suffix=none) %}
    {%- if suffix is none -%} {%- set suffix="__dw_tmp" -%} {%- endif -%}
    {%- set temp_relation = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier ~ suffix) -%}
    {%- do return(temp_relation) -%}
{% endmacro %}

{% macro dw_make_current_view_relation(relation) %}
    {%- set view_identifier -%})
        {{ relation.identifier | upper | replace("T_", "V_", 1) }}_CURRENT
    {%- endset -%}
    {%- set view_relation = api.Relation.create(database=relation.database, schema=relation.schema, identifier=view_identifier) -%}
    {%- do return(view_relation) -%}
{% endmacro %}

{% macro dw_get_relation(relation) %}
    {%- set db_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
    {%- do return(db_relation) -%}
{% endmacro %}

{% macro dw_truncate_table(relation) -%}
    {%- set is_delete = dw_get_meta(config).get("is_delete", False) -%}

    {%- if is_delete -%}
        delete from {{ relation }}
    {%- else -%}
        truncate table {{ relation }}
    {%- endif -%}
{%- endmacro %}

{% macro dw_insert_into_table(relation, sql) -%}
    insert into {{ relation }} {{ sql }}
{%- endmacro %}

{# /* macro to generate statement for cleaning working objects */ #}
{% macro dw_drop_views(relations) -%}
    {%- for relation in relations -%}
        drop view {{ relation }}{{"; " if not loop.last}}
    {%- endfor -%}
{%- endmacro %}

{% macro dw_alter_table_primary_key_constriant(relation, key_columns, action="add") -%}
    {%- set alter_key_statement -%}
        alter table {{ relation }} {{ action }} constraint pk_{{ relation | replace(".", "_") }} primary key ({{ dw_list_to_string(key_columns) }})
    {%- endset -%}

    {%- do return(alter_key_statement) -%}
{%- endmacro %}

{% macro dw_alter_table_unique_constriant(relation, key_columns, action="add") -%}
    {%- set alter_key_statement -%}
        alter table {{ relation }} {{ action }} constraint uk_{{ relation | replace(".", "_") }} unique ({{ dw_list_to_string(key_columns) }})
    {%- endset -%}

    {%- do return(alter_key_statement) -%}
{%- endmacro %}

{% macro dw_alter_table_column_masking(relation, columns) -%}
    {%- set masking_policy = config.meta_get("masking_policy", default=none) -%}

    {%- set masking_columns = dw_generate_masking_policy_set(columns, masking_policy) -%}

    {%- if masking_columns is not none and masking_columns is defined -%}
        {%- set alter_masking_statement -%}
            {%- for masking in masking_columns -%}
            alter table {{ relation }} alter {{ masking }}{{"; "}}
            {%- endfor -%}
        {%- endset -%}
    {%- endif -%}

    {%- do return(alter_masking_statement) -%}
{%- endmacro %}

{# /* macro to generate statement for creating staging temporary view */ #}
{% macro dw_create_staging_view(relation, sql, suffix=none) -%}
    {%- set copy_grants = config.get("copy_grants", default=false) %}
    {%- set tmp_relation = dw_make_temp_relation(relation, suffix) -%}

    {%- call statement("build_staging_relation") -%}
        create or replace view {{ tmp_relation }}
        {% if copy_grants -%} copy grants {%- endif %}
        as 
        {{ sql }}
    {%- endcall -%}

    {%- do return(tmp_relation) -%}
{%- endmacro %}

{# /* macro to generate statement for creating staging temporary table */ #}
{% macro dw_create_staging_table(relation, sql, suffix=none, natural_keys=none, transform_type=none) -%}
    {%- set copy_grants = config.get("copy_grants", default=false) %}
    {%- set tmp_relation = dw_make_temp_relation(relation, suffix) -%}

    {%- call statement("build_staging_relation") -%}
        create or replace temporary table {{ tmp_relation }}
        {% if copy_grants -%} copy grants {%- endif %}
        as 
        {{ sql }}

        {%- if natural_keys is not none -%}
            ;
            {%- if transform_type in ["2_1", "2_2"] %}
                {{ dw_alter_table_primary_key_constriant(tmp_relation, ["dw_hash_key", "dw_strt_ts"], "add") }}
            {%- else -%}
                {{ dw_alter_table_primary_key_constriant(tmp_relation, ["dw_hash_key"], "add") }}
            {%- endif %}
        {%- endif -%}
    {%- endcall -%}

    {%- do return(tmp_relation) -%}
{%- endmacro %}

{# /* macro to generate key column list, when natural key is none, all columns are selected */ #}
{% macro dw_get_key_column_names(sql_columns, natural_keys=none, exclude_names=[]) -%} 
    {%- if natural_keys is none -%} 
        {%- set data_columns = dw_get_column_list(sql_columns, exclude_names) -%}   
        {%- set data_column_names = data_columns | map(attribute='name') -%}
        {%- set natural_keys = [] -%}
        {%- for column_name in data_column_names -%}
            {%- do natural_keys.append(column_name | lower) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- do return(natural_keys) -%}
{%- endmacro %}

{% macro dw_get_column_names(columns) -%} 
    {%- set column_names = [] -%}
    {%- set columns_name = columns | map(attribute='name') -%}
    {%- for column_name in columns_name -%}
        {%- do column_names.append(column_name | lower) -%}
    {%- endfor -%}

    {%- do return(column_names) -%}
{%- endmacro %}

{% macro dw_flatten_data(table) -%}
    select
        distinct seq
        , dw_process_ts
        , dw_session_id
        , data
    from
        {{ table }}
        , lateral flatten(data, recursive => true)
{%- endmacro %}

{% macro dw_extract_infa_columns(raw_table, data_column) %}
    select
        {{data_column}}:infa_operation_type::VARCHAR(1) as INFA_OPERATION_TYPE
      , TO_TIMESTAMP_NTZ({{data_column}}:infa_operation_time::VARCHAR,'YYYYMMDDHH24MISSFF') as INFA_OPERATION_TIME
      , {{data_column}}:infa_sortable_sequence::VARCHAR as INFA_SORTABLE_SEQUENCE
      , {{data_column}}:infa_transaction_id::VARCHAR as INFA_TRANSACTION_ID
      , {{data_column}}
    from
        {{ raw_table }}
{%- endmacro %}

{% macro dw_object_data_for_process(raw_relation) -%}

    {%- set countrows_query -%}
    select count(*) from {{ raw_relation }} d
    {%- endset -%}

    {%- set results = run_query(countrows_query) -%}

    {%- if execute -%}
        {%- set results_list = results.columns[0].values() -%}

    {%- else -%}
        {% set results_list = [0] -%}
    {%- endif %}

    {%- do return(results_list[0]) -%}

{%- endmacro %}

-- macros/get_column_values_from_query.sql
{% macro get_column_values_from_query(query, column) -%}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {% set column_values_sql %}
    with cte as (
        {{ query }}
    )
    select
      {{ column }} as value

    from cte
    group by 1
    order by 1 asc

    {% endset %}
  {#--  {{ log(column_values_sql, info=True) }}#}
    {%- set results = run_query(column_values_sql) %}
    {#--  {{ log(results, info=True) }}#}
    {% set results_list = results.columns[0].values() %}

   {#-- {{ log(results_list, info=True) }}#}
    {{ return(results_list) }}

{%- endmacro %}

{# /* macro to generate key column list, when natural key is none, all columns are selected */ #}
{% macro dw_generate_hash_key(key_column_names, hash_type) -%} 

    {%- set dbt_hash_case_sensitive_config = dw_get_meta(config).get("dbt_hash_case_sensitive", true) -%}
    {%- if hash_type == 'md5' -%}
        md5(upper(nvl(to_varchar({{ key_column_names | join("), '') || '|' || nvl(to_varchar(")}}), '')))::varchar(32)
    {%- elif hash_type == 'md5_binary' -%}
        {%- if dbt_hash_case_sensitive_config -%}
            md5_binary(concat_ws('|', nvl(to_varchar({{ key_column_names | join("), ''), nvl(to_varchar(") }}), '')))
        {%- else -%}
            md5_binary(upper(concat_ws('|', nvl(to_varchar({{ key_column_names | join("), ''), nvl(to_varchar(") }}), ''))))
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}