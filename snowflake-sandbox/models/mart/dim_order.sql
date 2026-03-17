with

orders as (

    select * from {{ ref('stg_orders') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

order_locations as (
    
    select
        orders.order_id,

        locations.location_name,

        locations.tax_rate * 100 as tax_rate_percent,
        orders.subtotal,
        orders.order_total,

        orders.ordered_at

    from orders

    inner join locations
        on orders.location_id = locations.location_id
)

select * from order_locations