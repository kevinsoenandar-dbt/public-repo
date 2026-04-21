
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select supply_uuid
from analytics.dbt_ksoenandar.stg_supplies
where supply_uuid is null



  
  
      
    ) dbt_internal_test