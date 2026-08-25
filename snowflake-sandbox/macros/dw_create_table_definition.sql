{# /* macro to generate snowflake table creation ddl statement */ #}
{% macro dw_create_table_definition(sql_columns, target_relation, hash_type, diff_type, audit_fields, temporary=False, natural_keys=none, source_type=none
    , transform_type=none, source_ts=none, check_delete=none, version_using_sort_sequence=none, period_start_date=none, period_end_date=none) -%}

    {%- set transient = config.get("transient", default=False) -%}
    {%- set cluster_by_keys = config.get("cluster_by") -%}
    {%- set enable_automatic_clustering = config.get("automatic_clustering", default=False) -%}
    {%- set copy_grants = config.get("copy_grants", default=False) -%}

    {#- /* extra table config */ -#}
    {%- set data_retention = dw_get_meta(config).get("data_retention", config.meta_get("data_retention", none)) -%}
    {%- set ddl_collation = config.meta_get("ddl_collation", default=none) -%}
    {%- set comment = dw_get_meta(config).get("comment", none) -%}

    {%- if (temporary or transient) and data_retention is not none -%}
        {%- if data_retention > 1 -%}
            {%- set data_retention = 1 -%}
        {%- elif data_retention < 0 %}
            {%- set data_retention = 0 -%}
        {%- endif -%}
    {%- endif -%}

    {#- /* cnostruct cluster key string */ -#}
    {%- set cluster_by_string = dw_list_to_string(cluster_by_keys) -%}

    {%- set exclude_names = dw_reserved_column_names() -%}

    {%- if source_type == "cdc" -%}
        {%- for name in dw_cdc_column_names()-%}
            {%- do exclude_names.append(name) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set data_columns = dw_get_column_list(sql_columns, exclude_names) -%}
    {%- set columns_create = dw_generate_column_create(data_columns) -%}

    create or replace {%- if temporary %} temporary{%- elif transient %} transient {%- endif %} table {{ target_relation }} 
    (
        dw_batch_id varchar default current_session() not null,
        dw_process_ts timestamp default current_timestamp() not null,
        {% if check_delete -%} 
            dw_rec_del_ind boolean default false not null,
        {%- endif %}
        {% if transform_type == "2_1" -%}
            dw_strt_ts timestamp not null,
        {%- endif %}
        {%- if transform_type == "2_2" %}
            dw_strt_ts timestamp not null,
            dw_end_ts timestamp default to_timestamp('9999-12-31 23:59:59.999999999') not null,
        {%- endif %}
        {#- /* always generate hash key */ -#}
        {% if hash_type == 'md5' %}
            dw_hash_key varchar(32) not null,
        {% elif hash_type == 'md5_binary' %}
            dw_hash_key binary(16) not null,            
        {% endif %}
        {% if diff_type == 'hash' %}
            dw_hash_diff binary(16) not null,        
        {% endif %} 
        {% if audit_fields == 'all' %}
            dw_dbt_invocation_id varchar,        
            {# dw_airflow_dag_run_id varchar, #}
        {% endif %}
        {% if version_using_sort_sequence == true -%}
            dw_sort_sequence binary(48) not null,
        {%- endif %}
        {% if period_start_date -%}
            period_start_date datetime not null,
        {%- endif %}
        {% if period_end_date -%}
            period_end_date datetime not null,
        {%- endif %}
        
        {{ columns_create }}
    )
    {% if cluster_by_string is not none -%} cluster by ({{ cluster_by_string }}) {%- endif %}
    {% if data_retention is not none -%} data_retention_time_in_days = {{ data_retention }} {%- endif %}
    {% if copy_grants -%} copy grants {%- endif %}
    {% if ddl_collation is not none -%} default_ddl_collation = '{{ ddl_collation }}' {%- endif %}
    {% if comment is not none -%} comment = '{{ comment }}' {%- endif %}

    {%- if natural_keys is not none -%}
        ;
        {%- if transform_type in ["2_1", "2_2"] %}
            {{ dw_alter_table_primary_key_constriant(target_relation, ["dw_hash_key", "dw_strt_ts"], "add") }}
        {%- else -%}
            {{ dw_alter_table_primary_key_constriant(target_relation, ["dw_hash_key"], "add") }}
        {%- endif %}
        ;
    {%- endif -%}
{%- endmacro %}