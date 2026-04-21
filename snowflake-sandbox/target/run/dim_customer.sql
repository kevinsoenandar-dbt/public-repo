
  
    



create or replace transient  table analytics.dbt_ksoenandar.dim_customer
    
    
    
    as (WITH stg_customers AS (
  /* Customer data with basic cleaning and transformation applied, one row per customer. */
  SELECT
    *
  FROM analytics.dbt_ksoenandar.stg_customers
), filter AS (
  SELECT
    *
  FROM stg_customers
  WHERE
    NOT CUSTOMER_NAME IS NULL
), dim_customer AS (
  SELECT
    *
  FROM filter
)
SELECT
  *
FROM dim_customer
    )
;




  