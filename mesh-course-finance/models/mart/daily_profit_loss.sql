with order_products as (
    select * from {{ ref("mesh_course_platform", "fct_order_products") }}
)

select
    date_trunc('day', order_date)::date as order_date,
    sum(product_price * order_product_quantity) as total_revenue,
    sum(product_cost * order_product_quantity) as total_cost,
    sum(product_price * order_product_quantity - product_cost * order_product_quantity) as total_profit

from order_products

group by 1