
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_count
from analytics.dbt_ksoenandar.fct_customer_segment_orders
where customer_count is null



  
  
      
    ) dbt_internal_test