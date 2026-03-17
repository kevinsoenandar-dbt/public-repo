with products as (
    select * from {{ ref("stg_bike_shop__products") }}
)

select
    product_id,
    product_name,
    product_material,
    product_category,
    product_subcategory

from products
