{{ config(materialized='table') }}

{#
  Aggregation rolling orders up to customer + region. Second downstream of
  int_customer_geo (the first is dim_customers) — pushes int_customer_geo over
  the downstream_count >= 2 gate for materialization_candidates.
#}

with orders as (
    select
        customer_id,
        sum(order_total)                as revenue_total,
        count(*)                        as order_count

    from {{ ref('stg_tpch__orders') }}
    group by 1
),

customer_geo as (
    select * from {{ ref('int_customer_geo') }}
)

select
    cg.customer_id,
    cg.customer_name,
    cg.region_name,
    cg.market_segment,
    coalesce(o.revenue_total, 0)        as revenue_total,
    coalesce(o.order_count, 0)          as order_count

from customer_geo cg
left join orders o on cg.customer_id = o.customer_id
