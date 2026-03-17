{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_product_id',
    table_type = 'iceberg',
    format = 'parquet'
)
}}

with order_products as (
    select *

    from {{ ref("stg_bike_shop__order_products") }}

    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
)

, products as (
    select * from {{ ref("stg_bike_shop__products") }}
)

, orders as (
    select * from {{ ref("stg_bike_shop__orders") }}
)

, transformed as (
    select
        order_products.order_product_id,
        order_products.product_id,
        order_products.order_id,
        orders.customer_id,
        orders.order_date,
        order_products.order_product_quantity,
        products.product_price,
        products.product_cost,
        order_products.loaded_at

    from order_products

    left join products
        on order_products.product_id = products.product_id

    left join orders
        on order_products.order_id = orders.order_id
)

select * from transformed
