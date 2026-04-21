with

source as (

    select * from jaffle_shop.raw.raw_customers

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name

    from source

)

select * from renamed