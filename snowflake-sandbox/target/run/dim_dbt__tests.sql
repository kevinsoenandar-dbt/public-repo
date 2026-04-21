
  create or replace   view analytics.dbt_ksoenandar.dim_dbt__tests
  
  
  
  
  as (
    with
    base as (

        select *
        from analytics.dbt_ksoenandar.stg_dbt__tests

    )

    , tests as (

        select
            test_execution_id
            , command_invocation_id
            , node_id
            , run_started_at
            , name
            , depends_on_nodes
            , package_name
            , test_path
            , tags
        from base

    )

select * from tests
  );

