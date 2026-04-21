
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_order_frequency
from analytics.dbt_ksoenandar.fct_customer_segment_orders
where avg_order_frequency is null



  
  
      
    ) dbt_internal_test