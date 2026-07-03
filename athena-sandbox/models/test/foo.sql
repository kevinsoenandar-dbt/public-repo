{{
    config(
        materialized="incremental",
        incremental_strategy="append",
    )
}}

select
    'foo' as id1,
    {{ utc_to_timezone("current_timestamp") }} as load_datetime
