
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_segment
from analytics.dbt_ksoenandar.int_customer_rfm_segments
where customer_segment is null



  
  
      
    ) dbt_internal_test