select * from (
select * from {{ source('jaffle_shop', 'raw_customers') }}
) as __preview_sbq__ limit 1000