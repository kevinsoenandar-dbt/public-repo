with order_products as (
    select * from {{ ref("mesh_course_platform", "fct_order_products") }}
),

products as (
    select * from {{ ref("mesh_course_platform", "dim_product") }}
)

select
    order_products.product_id,
    sum(order_product_quantity) as total_quantity,
    sum(product_price * order_product_quantity) as total_revenue,
    sum(product_price * order_product_quantity - product_cost * order_product_quantity) as total_profit

from order_products

inner join products
    on order_products.product_id = products.product_id

group by 1