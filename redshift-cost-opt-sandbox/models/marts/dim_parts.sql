{{ config(materialized='table') }}

{#
  ~30 MB — tier 0 'table'.
#}

select * from {{ ref('stg_tpch__parts') }}
