{{ config(
    materialized = "dw_table_scd",
    description = "Simple fixture-backed model for testing the dw_table_scd custom materialization.",
    meta = {
        'transform_type': "2_2",
        'natural_keys': ["customer_id"],
        'exclude_field_change': [],
        'source_type': "full",
        'hash_type': "md5_binary",
        'audit_fields': "all",
        'diff_type': "hash",
        'source_ts': "source_updated_at",
        'check_delete': true,
        'table_creation': true,
        'dbt_hash_case_sensitive': false
    },
    static_analysis = 'off'
) }}

{% set fixture_seed = var('scd_fixture_seed', 'scd_customer_profile_snapshot_1') %}

select
    customer_id,
    source_updated_at,
    customer_name,
    status,
    customer_tier,
    case when customer_tier = 'bronze' then 'low'
        when customer_tier = 'silver' then 'medium'
        when customer_tier = 'gold' then 'high'
    else 'unknown' end as classification,
    credit_limit
from {{ ref(fixture_seed) }}

