{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    dist='auto',
    sort='order_id'
) }}

{#
  ~1.7 GB at SF=10 — fits in the 1000–5000 MB merge band.
  Materialized as incremental MERGE so each dbt build emits MERGE events on the
  underlying table, making the observed DML pattern mutating (NOT append-mostly).
  Has a `unique` test declared on order_id (see _marts.yml) which populates
  int_dbt__unique_columns.

  Expected recommendation: tier 3 'merge' with unique_key = order_id.

  Sort key is order_id (numeric) — deliberately NOT a date type, so the
  sort_key_is_date check in the package's merge tier passes.
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
    o.order_clerk,
    o.ship_priority,
    cg.region_name,
    cg.market_segment

from orders o
left join customer_geo cg on o.customer_id = cg.customer_id

{% if is_incremental() %}
where o.order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }})
{% endif %}
