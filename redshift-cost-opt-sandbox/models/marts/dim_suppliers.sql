{{ config(materialized='table') }}

{#
  ~16 MB — tier 0 'table'.
#}

with suppliers as (
    select * from {{ ref('stg_tpch__suppliers') }}
),

nations as (
    select * from {{ ref('stg_tpch__nations') }}
)

select
    s.supplier_id,
    s.supplier_name,
    s.account_balance,
    n.nation_name

from suppliers s
inner join nations n on s.nation_id = n.nation_id
