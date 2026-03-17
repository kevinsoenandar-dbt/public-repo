with customers as (
    select * from {{ ref("stg_bike_shop__customers") }}
)

select
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email_address,
    customer_gender,
    customer_city

from customers
