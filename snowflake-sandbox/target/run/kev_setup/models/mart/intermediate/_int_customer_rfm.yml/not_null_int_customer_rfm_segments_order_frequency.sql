
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_frequency
from analytics.dbt_ksoenandar.int_customer_rfm_segments
where order_frequency is null



  
  
      
    ) dbt_internal_test