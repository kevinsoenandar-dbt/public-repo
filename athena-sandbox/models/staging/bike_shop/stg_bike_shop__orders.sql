with source as (
    select * from {{ source("bike_shop", "orders") }}
),

renamed as (

    select

        ----------  ids
        id as order_id,
        customer_id,

        ---------- text
        array_join(
    transform(
        split(lower(order_status), ' '), 
        x -> concat(upper(substr(x, 1, 1)), substr(x, 2))
    ), 
    ' '
) as order_status,

        ---------- date
        order_date,

        ---------- timestamp
        loaded_at

    from source
)

select * from renamed
