with order_products as (
    select * from {{ ref("mesh_course_platform", "fct_order_products") }}
),

customers as (
    select * from {{ ref("mesh_course_platform", "dim_customer") }}
)

select
    order_products.customer_id,
    sum(order_product_quantity) as total_quantity,
    sum(product_price * order_product_quantity) as total_revenue,
    sum(product_cost * order_product_quantity) as total_cost,
    sum(product_price * order_product_quantity - product_cost * order_product_quantity) as total_profit

from order_products

inner join customers
    on order_products.customer_id = customers.customer_id

group by 1