
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lifetime_value
from analytics.dbt_ksoenandar.int_customer_rfm_segments
where lifetime_value is null



  
  
      
    ) dbt_internal_test