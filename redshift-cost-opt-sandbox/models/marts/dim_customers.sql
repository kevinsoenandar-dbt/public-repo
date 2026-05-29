{{ config(materialized='table') }}

{#
  ~250 MB — below the 1 GB incremental threshold, so expected tier 0 'table'.
  Has a `unique` test declared in _marts.yml (on customer_id) — verifies that
  int_dbt__unique_columns picks up the test even when the table isn't a merge
  candidate by size.
#}

select * from {{ ref('int_customer_geo') }}
