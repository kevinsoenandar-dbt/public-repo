{#
  View → view chain to test min_hops_to_table in int_redshift__view_chains.
  Layout:
    int_order_priorities      (view)  -- this model
      └── int_order_priorities_summary  (view)
            └── agg_order_priority_breakdown  (table)

  This view is 2 hops away from the materialized agg, so its min_hops_to_table
  should be 2 — meaningfully higher than int_customer_geo (1 hop).
#}

with orders as (
    select * from {{ ref('stg_tpch__orders') }}
),

customer_geo as (
    select * from {{ ref('int_customer_geo') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_total,
    o.order_date,
    o.order_priority,
    cg.region_name,
    cg.market_segment

from orders o
left join customer_geo cg on o.customer_id = cg.customer_id
