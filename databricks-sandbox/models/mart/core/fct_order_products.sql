{{ config(
    materialized = 'incremental',
    unique_key = 'order_product_id', 
    on_schema_change = 'append_new_columns',
    post_hook = '{{ unload_to_s3("fct_order_products", "dbt-ksoenandar", "mart_models") }}'
)
}}

with order_products as (
    select * 
    
    from {{ ref("stg_bike_shop__order_products") }}

    {% if is_incremental() %}
    where order_product_id not in (select order_product_id from {{ this }})
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
        products.product_cost * 0.95 as product_cost

    from order_products

    left join products
        on order_products.product_id = products.product_id

    left join orders
        on order_products.order_id = orders.order_id
)

select * from transformed