
    
    

with all_values as (

    select
        customer_segment as value_field,
        count(*) as n_records

    from analytics.dbt_ksoenandar.int_customer_rfm_segments
    group by customer_segment

)

select *
from all_values
where value_field not in (
    'Champions','Loyal Customers','Potential Loyalists','At-Risk','Hibernating'
)


