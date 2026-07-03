{#- /* experimental snowflake table materialization */ -#}
{% materialization dw_table_scd_v2, default -%}

    {%- set target_relation = this.incorporate(type="table") -%}
 
    {%- set hash_type = dw_get_meta(config).get("hash_type", "md5") -%}
    {%- set diff_type = dw_get_meta(config).get("diff_type", "column") -%}
    {%- set audit_fields = dw_get_meta(config).get("audit_fields", "basic") -%}

    {% set original_query_tag = set_query_tag() %}

{#- /* not run the main body if the sql text is blank */ -#}
{%- if sql | trim == "" %}
        {#- /* {%- do exceptions.raise_compiler_error("BLANK - SQL") -%} */ -#}
    {% call statement("main") %}
        select 1 as dummy
    {% endcall %}
    {%- do adapter.commit() -%}

{%- else -%}
{#- /* materialization configuration */ -#}

    {%- set natural_keys = dw_get_list(dw_get_meta(config).get("natural_keys", none)) -%}
    {%- set exclude_field_change = dw_get_list(dw_get_meta(config).get("exclude_field_change", none)) -%}
    {%- set check_delete = dw_get_meta(config).get("check_delete", True) -%}
    {%- set column_expansion = dw_get_meta(config).get("column_expansion", True) -%}

    {#- /* config validation */ -#}
    {%- set transform_type = dw_validate_config_transform_type(config) -%}
    {%- set source_type = dw_validate_config_source_type(config) -%}
    {%- set rollup_window = dw_validate_config_rollup_window(config) -%}
    {%- set source_ts = dw_validate_config_source_ts(config) -%}
    {%- set dedup_change_history = dw_validate_config_dedup_change_history(config) -%}
    {%- set version_using_sort_sequence = dw_validate_config_version_using_sort_sequence(config) -%}
    {%- set iics_delayed_record = dw_validate_iics_delayed_record(config) -%}
    {%- set retain_dw_process_ts = dw_validate_retain_process_ts(config) -%}

    {%- if version_using_sort_sequence and source_type != "cdc" -%}
        {%- set error_msg -%}
        Invalid version_using_sort_sequence value are provided: {{ version_using_sort_sequence }}
        Can only be set when source_type is "cdc" because it requires the cdc_seq_no source column.
        {%- endset -%}
        {%- do exceptions.raise_compiler_error(error_msg) -%}
    {%- endif -%}

    {%- if source_type == "cdc" -%}
        {%- set check_delete = True -%}
        {%- set dedup_change_history = False -%}
        {%- if source_ts is none or source_ts == "" -%}
            {%- set source_ts = "dtl__capxtimestamp" -%}
        {%- endif -%}
    {%- endif -%}

    {%- if version_using_sort_sequence %}
        {%- set cdc_seq_key = "cdc_seq_no" -%}
    {%- else %}
        {%- set cdc_seq_key = none -%}
    {%- endif -%}

{#- /* pre-hook placeholders */ -#}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

{#- /* begin main transaction */ -#}

    {#- /* check target table */ -#}
    {%- set existing_relation = load_relation(this) -%}
    {%- if existing_relation is not none and existing_relation.is_view -%}
        {{ log("Dropping relation " ~ existing_relation ~ " because it is a view and this model is a table.") }}
        {%- do adapter.drop_relation(existing_relation) %}
        {%- set existing_relation = none %}
    {%- endif -%}

    {%- set drop_relation = [] -%}

    {#- /* temp sql view */ -#}
    {%- set sql_relation = dw_create_staging_table(target_relation, sql, "__dw_sql") -%} 
    {%- set sql_columns = dw_get_column_in_relation(sql_relation) -%}

    {#- /* v2: infer omitted natural_keys once so full-refresh and incremental helpers use the same key list. */ -#}
    {#- /* Match dw_create_table_as_select inference by excluding control, CDC, and non-change-tracked fields from inferred keys. */ -#}
    {%- set natural_key_exclude_names = dw_reserved_column_names() -%}
    {%- if source_type == "cdc" -%}
        {%- for name in dw_cdc_column_names() -%}
            {%- do natural_key_exclude_names.append(name) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if exclude_field_change is not none -%}
        {%- for name in exclude_field_change -%}
            {%- do natural_key_exclude_names.append(name | lower) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set natural_keys = dw_get_key_column_names(sql_columns, natural_keys, natural_key_exclude_names) -%}

    {% if execute 
            and retain_dw_process_ts | lower in ["all rows", "exclude changed active"]%}
        {{ log(this ~ ' retain_dw_process_ts is ' ~ retain_dw_process_ts, info=True) }}
    {% endif %}

    {#- /* v2: retain_dw_process_ts on a full refresh needs a backup of the existing target before it is replaced. */ -#}
    {% if (retain_dw_process_ts in [ "all rows", "exclude changed active"] and  should_full_refresh() and transform_type == "2_2" and diff_type == 'hash' and existing_relation is not none) -%}
    {% set tz_local = modules.pytz.timezone('Australia/Perth') -%}
    {% set dt_local = modules.datetime.datetime.now(tz_local) -%}
    {% set today = dt_local.strftime("%Y%m%d") -%}
    {% set backup_table_name = this.name ~ '_bkp_' ~ today %}
    {% set backup_relation = api.Relation.create(database=this.database, schema=this.schema, identifier=backup_table_name, type='table') -%}
    {% call statement("backup_target_relation") %}
        create or replace temporary table {{ backup_relation }} clone {{ target_relation }}
    {% endcall %}
    {% else -%}
    {% set backup_relation = none -%}
    {% endif -%}

    {#- /* temp source staging */ -#}
    {%- set select_sql = dw_create_table_as_select(sql_relation, sql_columns, hash_type, diff_type, audit_fields, natural_keys, exclude_field_change
        , source_type, transform_type, source_ts, check_delete, rollup_window, dedup_change_history, cdc_seq_key, backup_relation,retain_dw_process_ts) -%}
    
    {%- if rollup_window is none -%}
        {%- set source_relation = dw_create_staging_view(target_relation, select_sql, "__dw_src") -%}
        {%- do drop_relation.append(source_relation) -%}
    {%- else -%}
        {%- set source_relation = dw_create_staging_table(target_relation, select_sql, "__dw_src", natural_keys, transform_type) -%}
    {%- endif -%}
    {%- set source_columns = dw_get_column_in_relation(source_relation) -%}

    {#- /* full load */ -#}
    {%- if existing_relation is none or flags.FULL_REFRESH -%}    
        
        {#- /* v2: missing target or --full-refresh rebuilds from current model schema instead of truncating/reusing an existing relation. */ -#}
        {%- set create_table_sql = dw_create_table_definition(sql_columns, target_relation, hash_type, diff_type, audit_fields, False
            , natural_keys, source_type, transform_type, source_ts, check_delete, version_using_sort_sequence) -%}            
        {% call statement("table_creation") %}
            {{ create_table_sql }}
        {% endcall %}

        {%- set target_columns = dw_get_column_in_relation(target_relation) -%}
        {#- /* v2: insert by explicit shared column names in target order to avoid positional drift after schema changes. */ -#}
        {%- set insert_columns = dw_get_common_column_list(source_columns, target_columns) -%}
        {%- set columns_select = dw_generate_column_select(insert_columns) -%}

        {%- set build_sql -%}
            {{ dw_alter_table_column_masking(target_relation, target_columns) }}
            insert into {{ target_relation }} ({{ columns_select }})
            select {{ columns_select }} from {{ source_relation }}
        {%- endset -%}

    {#- /* incremental load */ -#}
    {%- else -%}
        {#- /* table expansion */ -#}
        {%- if column_expansion -%}
            {%- set colum_updates = dw_get_column_updates(source_relation, target_relation) -%}
            
            {%- if colum_updates is not none and colum_updates != [] -%}
                {% call statement("column_sync") %}
                    {{ dw_generate_column_alter(colum_updates, target_relation) }}
                {% endcall %}  
            {%- endif -%}
        {%- endif -%}

        {%- set target_columns = dw_get_column_in_relation(target_relation) -%}

        {#- /* upsert source staging */ -#}
        {%- set upsert_sql = dw_scd_upsert_select(sql_relation, source_relation, source_columns, target_relation, target_columns, hash_type, diff_type, audit_fields
            , natural_keys, exclude_field_change, source_type, check_delete, transform_type, version_using_sort_sequence, iics_delayed_record)  -%}
        {%- set upsert_relation = dw_create_staging_table(target_relation, upsert_sql, "__edw_stg") -%} 
        {%- set upsert_columns = dw_get_column_in_relation(source_relation) -%}

        {%- set build_sql -%}
            {{ dw_alter_table_column_masking(target_relation, target_columns) }}
            {{ dw_scd_upsert(upsert_relation, upsert_columns, target_relation, natural_keys, transform_type, check_delete, version_using_sort_sequence) }}
        {%- endset -%}
        
    {%- endif -%}

    {% call statement("main") %}
        begin transaction;
        {{ build_sql }}
        ;
        commit;
        {{ dw_drop_views(drop_relation) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {%- do adapter.commit() -%}

{#- /* commit transaction */ -#}

{#- /* post-hook placeholder - not in transaction */ -#}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {%- do persist_docs(target_relation, model) -%}

{%- endif -%}

{#- /* Create the srci current view*/ -#}
    {%- set transform_type = dw_get_meta(config).get("transform_type") -%}

    {#- /* if the transform type is "1" then there's no dw_strt_ts column hence skipping that one*/ -#}
    {%- if transform_type | lower in [ "2_1"] %}

        {#- /* Reuse normalized natural_keys from the materialization path; only fall back for blank-SQL/no-op runs. */ -#}
        {%- if natural_keys is not defined -%}
            {%- set natural_keys = dw_get_list(dw_get_meta(config).get("natural_keys", none)) -%}
        {%- endif -%}
        {%- set sql_columns = dw_get_column_in_relation(target_relation) -%}
        {%- set partition_cols -%}
            {% for col in natural_keys -%}
                {{ col }}{{" , " if not loop.last}}
            {%- endfor %}
        {%- endset -%}
        {%- set select_cols -%}
            {% for col in sql_columns -%}
                {%- if col.name | lower not in ["dw_strt_ts"] %}
                   {{ col.name }}{{" , " if not loop.last}}
                {%- endif -%}
            {%- endfor %}
        {%- endset -%}

        {% call statement("create source current view") %}
            create or replace view {{target_relation}}_curr_v as (
                select  dw_strt_ts, 
                        {{select_cols}}
                from {{target_relation}}
                qualify row_number() over (partition by {{partition_cols}} order by dw_strt_ts desc) = 1
            )
        {% endcall %}

        {% call statement("create source historical view") %}
            create or replace view {{target_relation}}_hist_v as (
                select dw_strt_ts,
                       {% if transform_type == '2_1' %}
                            lead(dw_strt_ts, 1, '9999-12-31 23:59:59.999999999') over (partition by {{partition_cols}} order by dw_strt_ts) as dw_end_ts,
                       {% endif %}
                       {{select_cols}}
                from {{target_relation}}
            )
        {% endcall %}

    {%- endif -%}
    
    {{ return({"relations": [target_relation]}) }}

    {% do unset_query_tag(original_query_tag) %}
    
{%- endmaterialization %}
