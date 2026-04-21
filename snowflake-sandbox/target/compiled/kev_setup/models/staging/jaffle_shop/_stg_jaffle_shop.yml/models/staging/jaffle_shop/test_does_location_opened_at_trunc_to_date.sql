with

 __dbt__cte__jaffle_shop__raw_stores as (

-- Fixture for raw_stores
select 
    
        try_cast('1' as character varying(16777216))
     as "ID", 
    
        try_cast('Vice City' as character varying(16777216))
     as "NAME", 
    
        try_cast('2016-09-01T00:00:00' as TIMESTAMP_NTZ)
     as "OPENED_AT", 
    
        try_cast('0.2' as FLOAT)
     as "TAX_RATE"
union all
select 
    
        try_cast('2' as character varying(16777216))
     as "ID", 
    
        try_cast('San Andreas' as character varying(16777216))
     as "NAME", 
    
        try_cast('2079-10-27T23:59:59.9999' as TIMESTAMP_NTZ)
     as "OPENED_AT", 
    
        try_cast('0.1' as FLOAT)
     as "TAX_RATE"
), source as (

    select * from __dbt__cte__jaffle_shop__raw_stores

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- text
        name as location_name,

        ---------- numerics
        tax_rate,

        ---------- timestamps
        date_trunc('day', opened_at) as opened_date

    from source

)

select * from renamed