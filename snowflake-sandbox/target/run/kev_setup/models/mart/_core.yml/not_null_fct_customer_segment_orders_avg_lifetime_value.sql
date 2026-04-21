
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_lifetime_value
from analytics.dbt_ksoenandar.fct_customer_segment_orders
where avg_lifetime_value is null



  
  
      
    ) dbt_internal_test