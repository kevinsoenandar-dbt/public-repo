with source as (

    select
        ps_partkey      as part_id,
        ps_suppkey      as supplier_id,
        ps_availqty     as available_quantity,
        ps_supplycost   as supply_cost,
        ps_comment      as partsupp_comment

    from {{ source('tpch', 'partsupp') }}

)

select * from source
