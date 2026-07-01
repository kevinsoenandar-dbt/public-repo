{# /* macro to generate select statement which adds control fields and rollup capability to the original dataset */ #}
{% macro dw_create_table_as_select(sql_relation, sql_columns, hash_type, diff_type, audit_fields, natural_keys=none, exclude_data_change=none, source_type=none
    , transform_type=none, source_ts=none, check_delete=none, rollup_window=none, dedup_change_history=none, cdc_seq_key=none, backup_relation=none, retain_dw_process_ts=none) -%}
    
    {%- set exclude_names = dw_reserved_column_names() -%}

    {%- if source_type == "cdc" -%}
        {%- for name in dw_cdc_column_names()-%}
            {%- do exclude_names.append(name) -%}
        {%- endfor -%}
    {%- endif -%}

    {# /* all the sql column except control columns are data column */ #}
    {%- set data_columns = dw_get_column_list(sql_columns, exclude_names) -%}
    {%- set columns_select = dw_generate_column_select(data_columns, "s") -%}

    {%- if exclude_data_change is not none -%}
        {%- for name in exclude_data_change -%}
            {%- do exclude_names.append(name | lower) -%}
        {%- endfor -%}
    {%- endif -%}

    {# /* get the key column, if not set then all the data column except those not for change tracking */ #}
    {%- set key_column_names = dw_get_key_column_names(sql_columns, natural_keys, exclude_names) -%}

    {%- if transform_type in ["2_1", "2_2"] and dedup_change_history is not none -%}
        {# /* get data colomns for change tracking */ #}
        {%- for name in key_column_names -%}
            {%- do exclude_names.append(name | lower) -%}
        {%- endfor -%}

        {%- set data_change_columns = dw_get_column_list(sql_columns, exclude_names) -%}
        {%- set change_column_names = dw_get_column_names(data_change_columns) -%}
        {%- if change_column_names == [] -%}
            {%- set change_column_names = key_column_names -%}
        {%- endif -%}
    {%- endif -%}

    with source_data as (
        {%- if transform_type in ["2_1", "2_2"] %}
        select 
            *
            {%- if dedup_change_history is not none %}
            , lag(dw_hash_diff) over (partition by {{ key_column_names | join(", ") }} order by dw_strt_ts asc) as dw_hash_diff_prev
                {%- if check_delete %} , lag(dw_rec_del_ind) over (partition by {{ key_column_names | join(", ") }} order by dw_strt_ts asc) as dw_rec_del_ind_prev  {%- endif %}
            {%- endif %}
        from
        (
        {%- endif %}
        select 
            {{ dw_generate_hash_key(key_column_names, hash_type) }} as dw_hash_key
            {%- if transform_type in ["2_1", "2_2"] and dedup_change_history is not none %}
                , {{ dw_generate_hash_key(change_column_names, hash_type) }} as dw_hash_diff
            {%- endif %}
            , {% if rollup_window is not none -%} date_trunc({{ rollup_window }}, {%- endif -%}
            {%- if source_ts is none -%} current_timestamp() {%- else -%} {{ source_ts }} {%- endif -%}
            {%- if rollup_window is not none -%} ) {%- endif -%}::timestamp as dw_strt_ts 
            {%- if cdc_seq_key is none -%}/**/ {%- else %}, {{ cdc_seq_key }} as dw_sort_sequence {%- endif -%}
            {%- if check_delete %}
                {%- if source_type == "cdc" -%}
                    , (case when upper(dtl__capxaction) = 'D' then true else false end)
                {%- else %}
                    , false
                {%- endif %}::boolean as dw_rec_del_ind
            {%- endif %}
            , s.* 
        from {{ sql_relation }} s
        {%- if transform_type in ["2_1", "2_2"] and source_type == "cdc" %}
        qualify row_number() over 
            (partition by {{ key_column_names | join(", ") }}, date_trunc(day, {{ source_ts }}) 
            order by {{ source_ts }} desc, infa_process_timestamp desc nulls last, cdc_seq_no desc nulls last ) = 1{%- endif %} 

        {%- if transform_type in ["2_1", "2_2"] %}
        )
        {%- endif %}
    )
    {%- if backup_relation is not none %}
    , backup_table as (
    select 
    dw_hash_key
    , dw_hash_diff
    , dw_strt_ts
    , dw_process_ts
    from
    {{ backup_relation }}
    )
    {%- endif %}
    , processed_source_data as (
    select 
        current_session() as dw_batch_id,
        {%- if check_delete %}
        s.dw_rec_del_ind,
        {%- endif %}
        {%- if transform_type in ["2_1"] %} 
        s.dw_strt_ts, 
        {%- elif transform_type == "2_2" %} 
        s.dw_strt_ts, 
        {% if rollup_window is not none -%}date_trunc({{ rollup_window }}, {%- endif -%}
        {%- if source_ts is none -%} to_timestamp('9999-12-31 23:59:59.999999999') {%- else -%}
        lead(dateadd(nanosecond, -1 ,s.dw_strt_ts), 1, '9999-12-31 23:59:59.999999999') over (partition by {% for col in key_column_names %}s.{{ col }}{{ ", " if not loop.last }}{% endfor %} order by s.dw_strt_ts asc, s.{{ source_ts }} desc) 
        {%- endif %}
        {%- if rollup_window is not none -%} ) {%- endif -%}::timestamp as dw_end_ts,
        {%- endif %} 
        s.dw_hash_key, 
        {%- if diff_type == 'hash'%}
        s.dw_hash_diff,
        {%- endif %}
        {%- if audit_fields == 'all'%}
        '{{invocation_id}}' as dw_dbt_invocation_id,
        {# '{{ get_dag_run_id() }}' as dw_airflow_dag_run_id, #}
        {%- endif %}
        {%- if cdc_seq_key is not none -%}
            {{ cdc_seq_key }} as dw_sort_sequence,
        {%- endif %}
        {%- if backup_relation is not none and retain_dw_process_ts == "all rows" %}
        {#- /* retain_dw_process_ts: scenario1  : retain dw_process_ts from backup table for all rows (no filters applied) */ #}
        coalesce(b.dw_process_ts, current_timestamp()::timestamp) as dw_process_ts,
        {%- elif backup_relation is not none and retain_dw_process_ts == "exclude changed active" %} 
        {#- /* retain_dw_process_ts: scenario2  : for active row, if hash_diff is changed, generate new timestamp, otherwise default to old dw_process_ts */ #}
        case when dw_end_ts = to_timestamp('9999-12-31 23:59:59.999999999') and s.dw_hash_diff <> b.dw_hash_diff then current_timestamp()::timestamp else coalesce(b.dw_process_ts, current_timestamp()::timestamp) end as dw_process_ts,
        {%- else %}
        {#- /* retain_dw_process_ts: scenario3  : bau scenario */ #}
        current_timestamp()::timestamp as dw_process_ts, 
        {%- endif %}
        {{ columns_select }}
    from  source_data s
    {% if backup_relation is not none -%}
    left join backup_table b on s.dw_hash_key = b.dw_hash_key and s.dw_strt_ts = b.dw_strt_ts
    {% endif -%}
    {% if transform_type in ["2_1", "2_2"] and dedup_change_history is not none -%} 
    where 1=0
    or s.dw_hash_diff_prev is null 
    or s.dw_hash_diff_prev <> s.dw_hash_diff
    {#- /* Handle scenario when delete operation is performed as the last capxaction */ #}
    {%- if check_delete %} 
    or ( s.dw_rec_del_ind <> s.dw_rec_del_ind_prev )
    {%- endif %}
    {%- endif %}
    )
    select 
        psd.dw_batch_id
        , psd.dw_process_ts 
        {% if check_delete %}, psd.dw_rec_del_ind {% endif %}
        {% if transform_type in ["2_1", "2_2"] %}, psd.dw_strt_ts {%- endif %}
        {% if transform_type in ["2_2"] %}, psd.dw_end_ts {%- endif %}
        , psd.dw_hash_key 
        {% if diff_type == 'hash'%}, psd.dw_hash_diff {%- endif %}
        {%- if audit_fields == 'all'%}
        , psd.dw_dbt_invocation_id
        {#, psd.dw_airflow_dag_run_id #}
        {%- endif %}
        {%- if cdc_seq_key is not none %}, psd.dw_sort_sequence {%- endif %}
        , {{dw_generate_column_select(data_columns, "psd")}}
    from processed_source_data psd
{%- endmacro %}