{#
  View joining lineitem to orders + customers. Used by 2+ downstream fact/agg
  marts (fct_lineitem, agg_daily_sales). Should trigger materialization_candidates
  via the downstream_count gate.

  This view is also expensive to re-scan (joins 60M-row lineitem to 15M-row
  orders to 1.5M-row customers), so once it accumulates query traffic, the
  total_gb_rescanned signal will be high.
#}

with lineitem as (
    select * from {{ ref('stg_tpch__lineitem') }}
),

orders as (
    select * from {{ ref('stg_tpch__orders') }}
),

customers as (
    select * from {{ ref('stg_tpch__customers') }}
)

select
    li.order_id,
    li.line_number,
    li.event_date,
    li.quantity,
    li.extended_price,
    li.discount,
    li.tax,
    li.return_flag,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_priority,
    c.market_segment,
    c.nation_id

from lineitem li
inner join orders o    on li.order_id    = o.order_id
inner join customers c on o.customer_id  = c.customer_id
