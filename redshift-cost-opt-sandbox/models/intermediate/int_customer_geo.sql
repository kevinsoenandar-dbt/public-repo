{#
  View, with 2+ downstream marts (agg_customer_revenue, dim_customers,
  int_order_priorities). Should trigger materialization_candidates via the
  downstream_count gate (>= materialization_min_downstream_count, default 2).
#}

with customers as (
    select * from {{ ref('stg_tpch__customers') }}
),

nations as (
    select * from {{ ref('stg_tpch__nations') }}
),

regions as (
    select * from {{ ref('stg_tpch__regions') }}
)

select
    c.customer_id,
    c.customer_name,
    c.customer_address,
    c.account_balance,
    c.market_segment,
    n.nation_id,
    n.nation_name,
    r.region_id,
    r.region_name

from customers c
inner join nations n on c.nation_id = n.nation_id
inner join regions r on n.region_id = r.region_id
