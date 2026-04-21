
  create or replace   view analytics.dbt_ksoenandar.stg_orders
  
  
  
  
  as (
    with

source as (

    select * from jaffle_shop.raw.raw_orders

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        (subtotal / 100)::numeric(16, 2) as subtotal,
        (tax_paid / 100)::numeric(16, 2) as tax_paid,
        (order_total / 100)::numeric(16, 2) as order_total,

        ---------- timestamps
        date_trunc('day', ordered_at) as ordered_at

    from source

)

select * from renamed
  );

