with orders as (
    select * from {{ ref("stg_bike_shop__orders") }}
)

select
    order_id,
    order_status,
    order_date

from orders
