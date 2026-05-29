# redshift-cost-opt-sandbox

Test fixture project for the dbt-cost-optimization-package Redshift v1.
Each model in this project is deliberately structured to trigger a specific
recommendation tier from the package. Run the package against this project,
inspect the outputs, and verify the package emits the expected recommendations.

## Scope

- **Platform:** Redshift (RA3 provisioned or Serverless — v1 scope).
- **Source data:** TPC-H scale factor 10, already loaded into the `tpch`
  database, `raw_data` schema.
- **dbt CLI:** uses the dbt platform CLI (formerly dbt Cloud CLI). No
  `profiles.yml` needed — connection is resolved via `dbt_cloud.yml` and
  your dbt platform login.

## Setup

1. Authenticate against dbt platform (`dbt environment login` or similar).
2. From this directory, run `dbt deps` to pull in the cost-optimization package
   (installed via local path — see `packages.yml`).
3. `dbt parse` to confirm the project compiles before doing any expensive
   runs against the cluster.

## Project layout

```
models/
├── _sources.yml                       tpch.raw_data source
├── staging/                           passthrough views; column renames only
│   ├── stg_tpch__regions.sql
│   ├── stg_tpch__nations.sql
│   ├── stg_tpch__suppliers.sql
│   ├── stg_tpch__customers.sql
│   ├── stg_tpch__parts.sql
│   ├── stg_tpch__partsupp.sql
│   ├── stg_tpch__orders.sql
│   └── stg_tpch__lineitem.sql         l_shipdate aliased to event_date
├── intermediate/                      views designed to trigger materialization_candidates
│   ├── int_customer_geo.sql           2+ downstream → downstream_count gate
│   ├── int_lineitem_enriched.sql      2+ downstream → downstream_count gate
│   ├── int_order_priorities.sql       view at chain depth 2 from terminus
│   └── int_order_priorities_summary.sql  view at chain depth 1 from terminus
└── marts/
    ├── dim_regions.sql                tiny table → tier 0
    ├── dim_nations.sql                tiny table → tier 0
    ├── dim_suppliers.sql              ~16 MB → tier 0
    ├── dim_parts.sql                  ~30 MB → tier 0
    ├── dim_customers.sql              ~250 MB, has unique test → tier 0
    ├── fct_orders.sql                 ~1.7 GB, incremental merge, unique test → tier 3 merge
    ├── fct_lineitem.sql               ~7 GB, append-mostly, event_date → tier 1 microbatch
    ├── agg_order_priority_breakdown.sql  terminus of view chain
    ├── agg_customer_revenue.sql       downstream of int_customer_geo
    └── agg_daily_sales.sql            downstream of int_lineitem_enriched
```

## Fixture → recommendation mapping

### `int_redshift__materialization_candidates`

| Fixture | Trigger | Expected `is_candidate` |
|---|---|---|
| `int_customer_geo` | 2 downstream models (`dim_customers`, `agg_customer_revenue`, `int_order_priorities`) | `true` (downstream gate) |
| `int_lineitem_enriched` | 2 downstream models (`fct_lineitem`, `agg_daily_sales`) | `true` (downstream gate) |
| `int_order_priorities` | 1 downstream (view) — under both gates without external traffic | `false` until queried |
| `int_order_priorities_summary` | 1 downstream (table) | `false` until queried |
| `stg_tpch__*` views | Each has at least 1 downstream | Most fall below gate unless query traffic accumulates |

### `int_redshift__view_chains` — `min_hops_to_table`

| View | Expected `min_hops_to_table` |
|---|---|
| `int_order_priorities` | 2 (→ summary → agg) |
| `int_order_priorities_summary` | 1 (→ agg) |
| `int_lineitem_enriched` | 1 (→ fct_lineitem) |
| `int_customer_geo` | 1 (→ dim_customers) |

### `int_redshift__incremental_strategy_recommendations`

| Fixture | Tier | Reason |
|---|---|---|
| `dim_regions`, `dim_nations`, `dim_suppliers`, `dim_parts` | **0 'table'** | Far under `incremental_min_table_size_mb` (1000 MB) |
| `dim_customers` | **0 'table'** | ~250 MB, still under 1 GB |
| `fct_orders` | **3 'merge'** | ~1.7 GB, has `unique` test on `order_id`, sort by `order_id` (non-date), observed DML pattern = MERGE → not append-mostly |
| `fct_lineitem` | **1 'microbatch'** | ~7 GB, `event_date` column present, observed DML pattern = CTAS-only → append-mostly, > 1M rows |

### Tiers not yet covered

- **Tier 2 'append'**: would need a > 1 GB append-mostly model with no
  recognized `event_time` column name. Easy to add: clone `fct_lineitem` and
  drop the `event_date` rename in the staging layer (e.g., rename to
  `ship_date` instead).
- **Tier 4 'delete+insert'**: would need a > 5 GB mutable model without a
  single-column `unique` test. Could add a denormalized `fct_lineitem_history`
  materialized as incremental merge in dbt config (so MERGE events fire) but
  with the test declared as `dbt_utils.unique_combination_of_columns` instead
  of `unique` (which the package's v1 doesn't parse).

## How to actually exercise the package against this project

`dbt build` alone is not enough. The package's signals depend on observed
query activity recorded in `sys_query_history` and `sys_query_detail`, both
of which retain only the recent window (`materialization_lookback_days: 7`,
`incremental_lookback_days: 14`).

To exercise the package end-to-end you need to:

1. **Build the project once** so all the relations exist:
   `dbt build`

2. **Generate read traffic against the views** so the materialization-candidate
   attribution model has queries to attribute. Run a script that hits the
   intermediate views directly by name, repeatedly, over a few days:
   ```sql
   -- Run this loop, e.g., from a separate session, 100x/day for 7 days
   select count(*) from analytics.int.int_customer_geo;
   select * from analytics.int.int_lineitem_enriched limit 1000;
   ...
   ```
   The package's `int_redshift__query_view_access` requires both a scan
   superset match (sys_query_detail) AND a text match against `schema.view`,
   so the queries must reference the views by their fully-qualified name.

3. **Re-run `dbt build` periodically** (daily) so the table_query_stats_daily
   has multiple days of DML events for `fct_orders` (MERGE) and `fct_lineitem`
   (CTAS).

4. **Wait for the lookback window** — at least 7 days for materialization
   recommendations, 14 days for incremental strategy recommendations. Earlier
   inspection works but the signals will be sparse.

5. **Build the package's models**:
   ```
   dbt build --select package:dbt_cost_optimization_package
   ```
   Then inspect the marts:
   ```sql
   select * from <pkg_schema>.fct_redshift__warehouse_optimization_recommendations;
   select * from <pkg_schema>.int_redshift__materialization_candidates;
   select * from <pkg_schema>.int_redshift__incremental_strategy_recommendations;
   ```

## Cost notes

- TPC-H SF=10 at ~10 GB total is small enough that full rebuilds are cheap on
  Serverless. Each `dbt build` ≈ a few dollars of compute on a 32-RPU workgroup.
- The view-chain recursion in `int_redshift__view_chains` and the dependency
  walk in `int_redshift__view_dependencies` are catalog-only operations and
  effectively free.
- `int_redshift__query_view_access` is the most expensive package model — it
  joins `sys_query_detail` to `int_redshift__view_dependencies` with an ILIKE
  text match. Run it on a small warehouse off-peak.

## Branch

This fixture lives on `feat/redshift-cost-optimization-sandbox`. Keep on this
branch while iterating; merge to `main` only when the package's recommendations
match the expected behavior described above for every fixture.
