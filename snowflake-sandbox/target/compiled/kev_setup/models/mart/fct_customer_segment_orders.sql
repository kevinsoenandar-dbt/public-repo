with segments as (
    select * from analytics.dbt_ksoenandar.int_customer_rfm_segments
),

customers as (
    select * from analytics.dbt_ksoenandar.dim_customer
)

select
    s.customer_segment,
    count(distinct s.customer_id)    as customer_count,
    round(avg(s.lifetime_value), 2)  as avg_lifetime_value,
    round(avg(s.order_frequency), 1) as avg_order_frequency,
    round(avg(s.recency_days), 0)    as avg_days_since_last_order
from segments s
left join customers c
    on s.customer_id = c.customer_id
group by 1
order by avg_lifetime_value desc