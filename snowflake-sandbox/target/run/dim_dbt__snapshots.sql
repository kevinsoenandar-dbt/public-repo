
  create or replace   view analytics.dbt_ksoenandar.dim_dbt__snapshots
  
  
  
  
  as (
    with
    base as (select * from analytics.dbt_ksoenandar.stg_dbt__snapshots),

    snapshots as (

        select
            snapshot_execution_id,
            command_invocation_id,
            node_id,
            run_started_at,
            name,
             database
            ,
             schema
            ,
            depends_on_nodes,
            package_name,
            path,
            checksum,
            strategy,
            meta,
            alias
        from base

    )

select *
from snapshots

  );

