with customers as (
    select * from {{ ref("stg_bike_shop__customers") }}
)

select * except(loaded_at)

from customers