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

> **⚠️ Architecture note (updated after the Snowflake-parity refactor):**
> The tables below originally referenced `int_redshift__materialization_candidates`,
> `int_redshift__incremental_strategy_recommendations`, and `int_dbt__unique_columns`.
> All three were **retired**. The package now uses per-recommendation fact marts
> mirroring the Snowflake implementation. The mapping below reflects the
> current architecture. See `docs/redshift/materialization-strategy.md` in the
> package repo for the full policy history.

### `fct_redshift__table_materialization_candidates`
*(replaces the retired `int_redshift__materialization_candidates`)*

| Fixture | Trigger | Signal(s) to check |
|---|---|---|
| `int_customer_geo` | 2 downstream models (`dim_customers`, `agg_customer_revenue`, `int_order_priorities`) | `composite_chain_score`, `min_hops_to_table`, `downstream_table_count` — populate from view-chain structure alone, no query traffic needed |
| `int_lineitem_enriched` | 2 downstream models (`fct_lineitem`, `agg_daily_sales`) | Same as above |
| `int_order_priorities` | 1 downstream (view), chain depth 2 from terminus | `min_hops_to_table = 2` |
| `int_order_priorities_summary` | 1 downstream (table), chain depth 1 from terminus | `min_hops_to_table = 1` |
| `stg_tpch__*` views | Each has at least 1 downstream | `select_count` / `attribution_confidence` require the manual query-traffic step below — these are query-activity-dependent, unlike the chain-structure signals above |

**Two independent signal groups in this mart** — don't conflate them:
- **Structural** (`composite_chain_score`, `min_hops_to_table`, `downstream_table_count`) — derived from `int_redshift__view_chains`, populate immediately after `dbt build`, no query traffic required.
- **Activity-based** (`select_count`, `attribution_confidence`, via `int_redshift__query_view_access`) — require the manual repeated-query step in "How to actually exercise the package" below.

### `fct_redshift__incremental_materialization_candidates`
*(replaces the retired `int_redshift__incremental_strategy_recommendations` — Model 1 of 2, "should this table be incremental")*

**Scope note:** this mart only considers models where `materialized = 'table'` in the dbt manifest (mirrors Snowflake's design — finding TABLE models that should convert to INCREMENTAL). Models already materialized as `incremental` (like `fct_orders`) are permanently out of scope for this mart, by design — there is nothing to recommend for a table that is already incremental.

| Fixture | In scope? | Notes |
|---|---|---|
| `dim_regions`, `dim_nations`, `dim_suppliers`, `dim_parts`, `dim_customers` | Yes (all `materialized='table'`) | Too small (well under `incremental_candidates_min_size_gb`, default 2 GB) — expected to **not** trigger. |
| `fct_lineitem` | **Yes** — the only meaningful candidate in this project | ~7 GB, `materialized='table'`. Needs `int_redshift__table_query_stats_daily` to have captured CTAS build events for it. See "known limitation" below. |
| `fct_orders` | **No — out of scope** | `materialized='incremental'`. Will never appear in this mart's output, regardless of query traffic. Do not use this fixture to test this pipeline. |

**Known limitation — build-history dependency:** the mart's trigger conditions (`triggered_by_build_time`, `triggered_by_size`, `triggered_by_compute_waste`) and its redundancy-rate tiering both depend on `int_redshift__table_query_stats_daily` having multiple CTAS build events for the table. A single `dbt build` gives exactly one data point:
- The mart *may* still surface `fct_lineitem` if a single build's size/duration crosses the trigger thresholds (`size_gb >= 2` combined with either `max_build_time_sec >= 300` or a compute-waste score built from a single build).
- The `rebuild_redundancy_rate` tiering (Strong Candidate / Candidate / Low ROI) requires `qualified_build_days >= 3` (default) — a static, one-shot TPC-H load will not satisfy this. Expect `'Candidate — Insufficient History'` unless you deliberately rebuild `fct_lineitem` (`dbt build --full-refresh --select fct_lineitem`) on 3+ separate days.

**Diagnostic query if this mart is empty:**
```sql
select * from <pkg_schema>.int_redshift__table_query_stats_daily
where table_name = 'fct_lineitem';
```
Zero rows here means the CTAS attribution pipeline itself has a gap (investigate before assuming "insufficient history" is the cause). A populated row with low `table_build_count`/`max_build_time_ms` confirms it's the expected build-history limitation, not a bug.

### `fct_redshift__incremental_config_recommendations`
*(replaces the retired unique-key detection based on `int_dbt__unique_columns` — Model 2 of 2, "what strategy/config")*

**This mart filters from `fct_redshift__incremental_materialization_candidates`'s output** (`where recommendation != 'Low ROI — Minimal Rebuild Redundancy'`). If Model 1 has zero rows for a table, Model 2 has nothing to build a recommendation from — check Model 1 first if this mart looks empty.

Unique-key and filter-column detection in this mart is **schema-driven, not query-traffic-driven** — it only needs the table and its columns to exist:
- Name-pattern detection scans column names for `id`, `*_id`, `*_key`, `*_sk` conventions to populate `best_unique_key`.
- A post-hook macro (`probe_unique_key_candidates`) then runs `APPROXIMATE COUNT(DISTINCT <col>)` against the real table to confirm uniqueness, populating `likely_unique_key`. **This only happens on a second run of the mart** — `likely_unique_key` is null after the first build.

| Fixture | Expected `best_unique_key` | Correctness check |
|---|---|---|
| `fct_lineitem` | Should be **null**, or should get downgraded on the probe if a false-positive column is matched | `lineitem`'s true key is composite (`order_id` + `line_number`); v1 only detects single-column keys. **This is the highest-value correctness check available on a single static load** — if the probe wrongly confirms `order_id` as unique on `lineitem` (it repeats once per line), that's a real bug. Confirm `incremental_strategy` downgrades to `append` rather than recommending `merge`/`delete+insert` on a non-unique key. |

Note: `fct_orders`'s `order_id` genuinely is a single-column unique key — a good positive-case check — but since `fct_orders` never appears in Model 1's output (see above), it will never appear here either. Don't expect to validate the "confirmed unique key" happy path against `fct_orders` under the current fixture set.

### Tiers not yet covered by this fixture set

The retired 5-tier decision tree (table/microbatch/append/merge/delete+insert) has been replaced by continuous scoring (`rebuild_redundancy_rate`, `compute_waste_score`) plus a separate strategy-selection mart. There is no longer a fixed "tier" concept to fill gaps for — instead, verify:
- **Append-only detection**: would need a `materialized='table'` model with `dml_count = 0` (insert-only DML pattern) and a build-history trail. Not currently fixture-covered.
- **Composite-key downgrade-to-append**: covered by `fct_lineitem` above — the most valuable open verification.

## How to actually exercise the package against this project

Not every signal needs waiting — split what you check by dependency:

### Checks available immediately after one `dbt build` (no waiting, no query traffic)

- **`fct_redshift__table_materialization_candidates`** structural signals
  (`composite_chain_score`, `min_hops_to_table`, `downstream_table_count`) —
  derived from `int_redshift__view_chains`, which is purely structural
  (project DAG shape), not activity-based.
- **`fct_redshift__incremental_config_recommendations`** unique-key /
  filter-column detection — schema-driven (column names + types). Run the
  mart **twice** (`dbt build --select fct_redshift__incremental_config_recommendations`
  a second time) to let the `probe_unique_key_candidates` post-hook populate
  `likely_unique_key`.
- **`fct_redshift__incremental_materialization_candidates`** may partially
  populate for `fct_lineitem` after a single build if the build crosses the
  trigger thresholds — but its redundancy-rate tiering needs multiple build
  days (see below).

### Checks that require observed query activity (`sys_query_history` / `sys_query_detail`)

Both retain only a recent rolling window (`table_materialization_lookback_days: 14`,
`incremental_candidates_lookback_days: 60` — see the package's `dbt_project.yml`
for current defaults).

1. **Build the project once** so all the relations exist: `dbt build`

2. **Generate read traffic against the views** so `fct_redshift__table_materialization_candidates`'s
   activity-based signals (`select_count`, `attribution_confidence`) have
   queries to attribute. Run a script that hits the intermediate views
   directly by name, repeatedly, over a few days:
   ```sql
   -- Run this loop, e.g., from a separate session, 100x/day for 7 days
   select count(*) from analytics.int.int_customer_geo;
   select * from analytics.int.int_lineitem_enriched limit 1000;
   ...
   ```
   The package's `int_redshift__query_view_access` requires both a scan
   superset match (`sys_query_detail`) AND a text match against `schema.view`,
   so the queries must reference the views by their fully-qualified name.

3. **Re-run `dbt build --full-refresh --select fct_lineitem`** on 3+ separate
   days so `int_redshift__table_query_stats_daily` accumulates enough CTAS
   build history (`qualified_build_days >= 3`) for
   `fct_redshift__incremental_materialization_candidates`'s
   `rebuild_redundancy_rate` tiering to move past
   `'Candidate — Insufficient History'`.

   Note: `fct_orders` is `materialized='incremental'`, not `table` — it is
   **permanently out of scope** for this mart regardless of how much you
   rebuild or query it. Only `materialized='table'` fixtures (`fct_lineitem`,
   the `dim_*` models) are candidates here.

4. **Wait for the lookback window** to accumulate enough history for the
   above. Earlier inspection works but the signals will be sparse.

5. **Build the package's models**:
   ```
   dbt build --select package:dbt_cost_optimization_package
   ```
   Then inspect the marts:
   ```sql
   select * from <pkg_schema>.fct_redshift__table_materialization_candidates;
   select * from <pkg_schema>.fct_redshift__incremental_materialization_candidates;
   select * from <pkg_schema>.fct_redshift__incremental_config_recommendations;
   select * from <pkg_schema>.fct_redshift__distkey_recommendations;
   select * from <pkg_schema>.fct_redshift__sortkey_recommendations;
   select * from <pkg_schema>.fct_redshift__vacuum_candidates;
   select * from <pkg_schema>.fct_redshift__analyze_candidates;
   ```

   **If `fct_redshift__incremental_materialization_candidates` or
   `fct_redshift__incremental_config_recommendations` come back empty**, check
   `int_redshift__table_query_stats_daily` for `fct_lineitem` first — a
   populated row with low build counts is expected (insufficient history);
   zero rows indicates a gap in CTAS attribution worth investigating before
   assuming it's just a timing issue.

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
