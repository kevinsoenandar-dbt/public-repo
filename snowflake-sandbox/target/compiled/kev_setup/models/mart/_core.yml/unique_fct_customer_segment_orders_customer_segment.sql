
    
    

select
    customer_segment as unique_field,
    count(*) as n_records

from analytics.dbt_ksoenandar.fct_customer_segment_orders
where customer_segment is not null
group by customer_segment
having count(*) > 1


