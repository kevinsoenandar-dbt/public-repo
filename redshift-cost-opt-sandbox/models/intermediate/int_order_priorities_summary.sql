{#
  Mid-chain view. 1 hop from the materialized downstream agg, 1 hop from a
  view parent. Pair with int_order_priorities to test min_hops_to_table values
  of 1 vs 2.
#}

with priorities as (
    select * from {{ ref('int_order_priorities') }}
)

select
    region_name,
    market_segment,
    order_priority,
    count(*)                            as order_count,
    sum(order_total)                    as revenue_total

from priorities
group by 1, 2, 3
