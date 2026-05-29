{{ config(materialized='table') }}

{#
  Small aggregation table, terminates the view chain:
    int_order_priorities (view)         → min_hops_to_table = 2
      └── int_order_priorities_summary (view)   → min_hops_to_table = 1
            └── agg_order_priority_breakdown (table)

  Use this pair when inspecting int_redshift__view_chains to confirm the
  recursive descent reports the expected min_hops_to_table values for each
  view in the chain.
#}

select * from {{ ref('int_order_priorities_summary') }}
