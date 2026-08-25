{# /* macro to generate statement for preparing upsert data select */ #}
{% macro dw_scd_upsert_select(sql_relation, source_relation, source_columns, target_relation, target_columns, hash_type, diff_type, audit_fields
    , natural_keys=none, exclude_data_change=none, source_type=none, check_delete=none, transform_type=none, version_using_sort_sequence=none, iics_delayed_record=none) -%}

    {%- set exclude_names = dw_reserved_column_names() -%}

    {%- if source_type == "cdc" -%}
        {%- for name in dw_cdc_column_names()-%}
            {%- do exclude_names.append(name) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set data_columns = dw_get_common_column_list(source_columns, target_columns, exclude_names) -%}
    {%- set target_columns_select = dw_generate_column_select(data_columns, "t") %}

    {%- if exclude_data_change is not none -%}
        {%- for name in exclude_data_change -%}
            {%- do exclude_names.append(name | lower) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set source_ts = dw_get_meta(config).get("source_ts", none) -%}
    {%- if source_ts is not none -%}
        {%- do exclude_names.append(source_ts | lower) -%}
    {%- endif -%}
    
    {% if diff_type == 'column' %}
        {%- set data_change_columns = dw_get_column_list(data_columns, exclude_names) -%}
        {%- set columns_compare = dw_generate_column_compare(data_change_columns, "s", "t") -%}
    {% elif diff_type == 'hash' %}
        {%- set columns_compare = "s.dw_hash_diff <> t.dw_hash_diff" -%}
    {% endif %}

    {%- set columns_select = dw_generate_column_select(source_columns) -%}

    {# /* Get the table with hash key */ #}
    {%- set join_column_string -%}
        {% for col in natural_keys -%}
            r.{{ col }} = t.{{ col }}{{" and " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {# /* Prepare the list of columns required to capture the full extract deleted records */ #}
    {%- set null_columns_full_extract -%}
        {% for col in natural_keys -%}
            r.{{ col }} is null {{"and " if not loop.last}}
        {%- endfor %}
    {%- endset -%}

    {# /* Set the comparison operator based on late arriving records */ #}
    {% set comparison_operator = ">=" if iics_delayed_record else ">" %}

    with target_data as (
        select t.* from {{ target_relation }} t
        inner join 
        ( select distinct {{ natural_keys | join(", ") }} from {{ sql_relation }} ) r
            on {{ join_column_string }}
        {%- if transform_type == "2_1" %}
        qualify row_number() over (partition by t.dw_hash_key order by t.dw_strt_ts desc nulls last) = 1
        {%- elif transform_type == "2_2" %} 
        where t.dw_end_ts = to_timestamp('9999-12-31 23:59:59.999999999')
        {%- endif %}
        {#- /* For full extract files, get the records deleted in source by comparing with srci */ -#}
        {%- if source_type == "full" %}  
        -- check if source is full extract
        UNION
        select t.* from {{ target_relation }} t 
        left join {{ sql_relation }} r
            on {{ join_column_string }}
        where {{null_columns_full_extract}}
        {%- if transform_type == "2_1" %}
        qualify row_number() over (partition by t.dw_hash_key order by t.dw_strt_ts desc nulls last) = 1
        {%- elif transform_type == "2_2" %} 
        and t.dw_end_ts = to_timestamp('9999-12-31 23:59:59.999999999')
        {%- endif %}        
        -- if loop closure for full extract check.
        {%- endif %}   
    )
    {#- /* ignore the record with timestamp in the past */ -#}
    {%- if transform_type in ["2_1", "2_2"] %}
    , source_in_future as (
        select s.* from {{ source_relation }} s
        left join target_data t
            on t.dw_hash_key = s.dw_hash_key
        where t.dw_hash_key is null or s.dw_strt_ts >= t.dw_strt_ts
    )
    {%- endif %}
    , source_data as ( 
        {%- if transform_type == "1" %}
        select * from {{ source_relation }}
        {%- else %}
        select 
            row_number() over (partition by dw_hash_key order by dw_strt_ts asc {%- if version_using_sort_sequence %}, dw_sort_sequence asc {%- endif %} ) as dw_rec_version_id
            , s.* 
        from source_in_future s
        {%- endif %}
    )
    {%- if transform_type == "1" %} 
    , update_data as (
        select 
            'update'::varchar(6) as dw_ops_type, s.*
        from source_data s
        inner join target_data t
            on t.dw_hash_key = s.dw_hash_key
        where ({{ columns_compare }}
        {#- /* deleted record re-created in source */ -#}
        {% if check_delete and source_type == "full" %}
            or t.dw_rec_del_ind = true -- if exists in source, the record must not deleted
        {% elif source_type == "cdc" %}
            or t.dw_rec_del_ind <> s.dw_rec_del_ind
        {%- endif %}
        )
    )
    {%- elif version_using_sort_sequence %} --handle  records that have same dw_strt_ts and a latter sort sequence
    , update_data as (
        select 
            'update'::varchar(6) as dw_ops_type, s.*
        from source_data s
        left join target_data t
            on t.dw_hash_key = s.dw_hash_key
        where t.dw_hash_key is not null
          and s.dw_rec_version_id >= 1
          and s.dw_strt_ts = t.dw_strt_ts 
          and s.dw_sort_sequence > t.dw_sort_sequence
          and {{ columns_compare }} 
    )
    {%- endif %}
    , insert_data as (
        select 
            'insert'::varchar(6) as dw_ops_type, s.*
        from source_data s
        left join target_data t
            on t.dw_hash_key = s.dw_hash_key
        where t.dw_hash_key is null
        {%- if transform_type in ["2_1", "2_2"] %}
            or s.dw_rec_version_id > 1
            {#- /* handling recurrence  of a deleted key for full extracts  */ -#}
            {%- if source_type == "full" %}
            or (s.dw_rec_version_id = 1 and s.dw_strt_ts > t.dw_strt_ts and t.dw_rec_del_ind=true )
            {%- endif %}
            or (s.dw_rec_version_id = 1 and s.dw_strt_ts {{ comparison_operator }} t.dw_strt_ts
                -- changed record
                and ({{ columns_compare }} {% if check_delete %} or s.dw_rec_del_ind <> t.dw_rec_del_ind {%- endif %} ))
        {%- endif %}
    )
    {#- /* find what's deleted in source. delta source will have check_delete as false. */ -#}
    {%- if check_delete and source_type == "full" %}
    , delete_data as (
        select 
            'delete'::varchar(6) as dw_ops_type 
            , current_session() as dw_batch_id
            , current_timestamp()::timestamp as dw_process_ts 
            , true::boolean as dw_rec_del_ind
            {%- if transform_type in ["2_1"]  %}
            , {% if source_ts is not none -%}nvl(bs.dw_source_ts, current_timestamp()){%- else -%}current_timestamp(){%- endif %}::timestamp as dw_strt_ts
            {%- elif transform_type == "2_2" %} 
            , {% if source_ts is not none -%}nvl(bs.dw_source_ts, current_timestamp()){%- else -%}current_timestamp(){%- endif %}::timestamp as dw_strt_ts
            , to_timestamp('9999-12-31 23:59:59.999999999') as dw_end_ts 
            {%- endif %}
            , t.dw_hash_key
            {% if diff_type == 'hash' %}
                , t.dw_hash_diff
            {% endif %}
            {% if audit_fields == 'all' %}
                , t.dw_dbt_invocation_id
                {#, t.dw_airflow_dag_run_id #}
            {% endif %}            
            , {{ target_columns_select }}
        from target_data t
        {%- if source_ts is not none %}
        left join (select {{ source_ts }} as dw_source_ts from source_data group by {{ source_ts }}) bs
            on bs.dw_source_ts > t.dw_strt_ts 
        {%- endif %}
        left join source_data s
            on t.dw_hash_key = s.dw_hash_key and t.dw_rec_del_ind = false
        where s.dw_hash_key is null 
        and  t.dw_rec_del_ind=false
        {%- if source_ts is not none %}
        qualify row_number() over (partition by t.dw_hash_key order by bs.dw_source_ts asc) = 1
        {%- endif %}
    )
    {%- endif %}

    {%- if transform_type == "1" or version_using_sort_sequence %}
    select dw_ops_type, {{ columns_select }} from update_data
    union all
    {%- endif %}
    select dw_ops_type, {{ columns_select }} from insert_data
    {%- if check_delete and source_type == "full" %}
    union all
    select dw_ops_type, {{ columns_select }} from delete_data
    {%- endif %}

{%- endmacro %}

{# /* macro to generate statement for upserting target table */ #}
{% macro dw_scd_upsert(upsert_relation, upsert_columns, target_relation, natural_keys=none, transform_type=none, check_delete=none, version_using_sort_sequence=none) -%}

    {%- set exclude_names = dw_reserved_column_names() -%}

    {%- if exclude_data_change is not none -%}
        {%- for name in exclude_data_change -%}
            {%- do exclude_names.append(name | lower) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set source_columns = dw_get_column_list(upsert_columns, exclude_names) -%}
    {%- set columns_assignment = dw_generate_column_assignment(source_columns, "s") -%}

    {%- set insert_columns = dw_get_column_list(upsert_columns, ["dw_ops_type"]) -%}
    {%- set columns_select = dw_generate_column_select(insert_columns) -%}

    {%- if transform_type == "1" -%}

        update {{ target_relation }} t
        set {{ columns_assignment }}
            , dw_batch_id = current_session()
            , dw_process_ts = current_timestamp()
            {%- if check_delete %}
            , dw_rec_del_ind = s.dw_rec_del_ind
            {%- endif %}
        from {{ upsert_relation }} s
        where s.dw_ops_type = 'update'
            and t.dw_hash_key = s.dw_hash_key
        ;

        {%- if check_delete %}
            update {{ target_relation }} t
            set dw_batch_id = current_session()
                , dw_process_ts = current_timestamp()
                , dw_rec_del_ind = true
            from {{ upsert_relation }} s
            where s.dw_ops_type = 'delete'
                and t.dw_hash_key = s.dw_hash_key
            ;
        {%- endif %}

        insert into {{ target_relation }} ({{ columns_select }})
        select {{ columns_select }} from {{ upsert_relation }}
        where dw_ops_type = 'insert'

    {%- elif transform_type == "2_1" and version_using_sort_sequence %}
        
        update {{ target_relation }} t
        set {{ columns_assignment }}
            , dw_batch_id = current_session()
            , dw_process_ts = current_timestamp()
            , t.dw_sort_sequence = s.dw_sort_sequence
        from {{ upsert_relation }} s
        where s.dw_ops_type = 'update'
            and t.dw_hash_key = s.dw_hash_key
        ;
        insert into {{ target_relation }} ({{ columns_select }})
        select {{ columns_select }} from {{ upsert_relation }} 
        where dw_ops_type in ('insert', 'delete')

    {%- elif transform_type == "2_1" %}

        insert into {{ target_relation }} ({{ columns_select }})
        select {{ columns_select }} from {{ upsert_relation }} 
        where dw_ops_type in ('insert', 'delete')

    {%- elif transform_type == "2_2" %}
        update {{ target_relation }} t
        set dw_end_ts = dateadd(nanosecond, -1 ,s.dw_strt_ts)
        from ( select dw_hash_key, min(dw_strt_ts) as dw_strt_ts from {{  upsert_relation  }} where dw_ops_type in ('insert', 'delete') group by 1 )  s
        where 1=1
            and t.dw_hash_key = s.dw_hash_key
            and t.dw_end_ts = timestamp '9999-12-31 23:59:59.999999999'
        ;

        insert into {{ target_relation }} ({{ columns_select }})
        select {{ columns_select }} from {{ upsert_relation }} 
        where dw_ops_type in ('insert', 'delete')
    {%- endif %}

{%- endmacro %}