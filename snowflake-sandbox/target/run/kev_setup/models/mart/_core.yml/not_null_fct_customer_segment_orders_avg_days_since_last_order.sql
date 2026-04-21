
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_days_since_last_order
from analytics.dbt_ksoenandar.fct_customer_segment_orders
where avg_days_since_last_order is null



  
  
      
    ) dbt_internal_test