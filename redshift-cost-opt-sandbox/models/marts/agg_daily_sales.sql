{{ config(materialized='table') }}

{#
  Second downstream consumer of int_lineitem_enriched (the first being
  fct_lineitem). Pushes int_lineitem_enriched over the downstream_count gate
  for materialization_candidates.

  Daily roll-up — moderate size, used by analysts directly. Once it accumulates
  ad-hoc query traffic, it'll contribute to total_gb_rescanned attribution for
  the underlying lineitem chain (via the package's scan-attribution model).
#}

select
    event_date,
    market_segment,
    sum(extended_price * (1 - discount))    as net_revenue,
    sum(quantity)                           as units_sold,
    count(distinct order_id)                as order_count

from {{ ref('int_lineitem_enriched') }}
group by 1, 2
