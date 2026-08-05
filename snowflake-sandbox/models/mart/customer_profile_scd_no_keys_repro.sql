{{ config(
    materialized = "dw_table_scd_v2",
    description = "Temporary repro model for testing dw_table_scd_v2 behavior when natural_keys is omitted.",
    meta = {
        'transform_type': "2_2",
        'exclude_field_change': [],
        'source_type': "full",
        'hash_type': "md5_binary",
        'audit_fields': "all",
        'diff_type': "hash",
        'source_ts': "source_updated_at",
        'check_delete': true,
        'version_using_sort_sequence': false,
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
    credit_limit
from {{ ref(fixture_seed) }}
