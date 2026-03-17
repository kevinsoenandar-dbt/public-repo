with source as (
    select * from {{ source("bike_shop", "customers") }}
),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        first_name as customer_first_name,
        last_name as customer_last_name,
        email as customer_email_address,
        case gender
            when 'F' then 'Female'
            when 'M' then 'Male'
            when 'X' then 'Non-binary'
        end as customer_gender,
        city as customer_city,

        ---------- timestamp
        loaded_at

    from source
)

select * from renamed
