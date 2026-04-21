
  
    

create or replace transient table analytics.dbt_ksoenandar.fct_order_items
    
  (
    order_item_id varchar,
    order_id varchar not null,
    product_id varchar not null,
    customer_id varchar not null,
    ordered_at timestamp_ntz not null,
    product_name varchar not null,
    product_price number(16,2) not null,
    is_food_item boolean not null,
    is_drink_item boolean not null,
    supply_cost number(29,2) not null
    
    )

    
    
    
    as (
    select order_item_id, order_id, product_id, customer_id, ordered_at, product_name, product_price, is_food_item, is_drink_item, supply_cost
    from (
        with

order_items as (

    select * from analytics.dbt_ksoenandar.stg_order_items

),


orders as (

    select * from analytics.dbt_ksoenandar.stg_orders

),

products as (

    select * from analytics.dbt_ksoenandar.stg_products

),

supplies as (

    select * from analytics.dbt_ksoenandar.stg_supplies

),

order_supplies_summary as (

    select
        product_id,

        sum(supply_cost * 1) as supply_cost -- latest change

    from supplies

    group by 1

),

joined as (

    select
        order_items.*,

        orders.customer_id,
        orders.ordered_at,

        products.product_name,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        order_supplies_summary.supply_cost

    from order_items

    left join orders on order_items.order_id = orders.order_id

    left join products on order_items.product_id = products.product_id

    left join order_supplies_summary
        on order_items.product_id = order_supplies_summary.product_id

)

select * from joined
    ) as model_subq
    )
;


  