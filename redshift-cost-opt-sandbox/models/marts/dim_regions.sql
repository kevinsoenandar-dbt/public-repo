{{ config(materialized='table') }}

{#
  Tiny table — well under incremental_min_table_size_mb (1000 MB) →
  expected to land in tier 0 'table' (leave-as-table) in the package output.
#}

select * from {{ ref('stg_tpch__regions') }}
