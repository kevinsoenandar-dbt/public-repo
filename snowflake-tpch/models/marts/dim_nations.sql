{{ config(materialized='table') }}

{#
  Tiny table — tier 0 'table'.
#}

select * from {{ ref('stg_tpch__nations') }}
