
    
    



select avg_days_since_last_order
from analytics.dbt_ksoenandar.fct_customer_segment_orders
where avg_days_since_last_order is null


