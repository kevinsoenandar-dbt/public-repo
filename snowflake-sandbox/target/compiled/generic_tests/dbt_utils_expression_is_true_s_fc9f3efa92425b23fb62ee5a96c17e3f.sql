



select
    1
from analytics.dbt_ksoenandar.stg_orders

where not(order_total - tax_paid = subtotal)

