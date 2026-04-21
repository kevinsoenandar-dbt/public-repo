
  create or replace   view analytics.dbt_ksoenandar.stg_dbt__tests
  
  
  
  
  as (
    with
    base as (

        select *
        from analytics.dbt_ksoenandar.tests

    )

    , enhanced as (

        select
            
md5(cast(coalesce(cast(command_invocation_id as TEXT), '') || '-' || coalesce(cast(node_id as TEXT), '') as TEXT)) as test_execution_id
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

select * from enhanced
  );

