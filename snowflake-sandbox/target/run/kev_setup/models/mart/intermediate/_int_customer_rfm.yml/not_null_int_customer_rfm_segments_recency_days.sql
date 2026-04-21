
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select recency_days
from analytics.dbt_ksoenandar.int_customer_rfm_segments
where recency_days is null



  
  
      
    ) dbt_internal_test