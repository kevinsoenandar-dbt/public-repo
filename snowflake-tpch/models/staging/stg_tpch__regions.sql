with source as (

    select
        r_regionkey     as region_id,
        r_name          as region_name,
        r_comment       as region_comment

    from {{ source('tpch', 'region') }}

)

select * from source
