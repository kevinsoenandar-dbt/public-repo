{{ config(
    materialized='table',
    dist='auto',
    sort='event_date'
) }}

{#
  ~7 GB at SF=10. Materialized as a regular table so each dbt build is a CTAS
  → observed DML is insert-only → is_append_mostly = true.

  Has the `event_date` column inherited from stg_tpch__lineitem (which aliased
  l_shipdate). That name is in the package's event_time precedence list, so
  event_time_column gets populated.

  Expected recommendation: tier 1 'microbatch' — large append-mostly with an
  event_time column, > 1M rows, > 1000 MB.
#}

select * from {{ ref('int_lineitem_enriched') }}
