WITH stg_customers AS (
  /* Customer data with basic cleaning and transformation applied, one row per customer. */
  SELECT
    *
  FROM {{ ref('kev_setup', 'stg_customers') }}
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