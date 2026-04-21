
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rfm_score
from analytics.dbt_ksoenandar.int_customer_rfm_segments
where rfm_score is null



  
  
      
    ) dbt_internal_test