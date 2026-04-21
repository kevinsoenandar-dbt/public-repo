-- Build actual result given inputs
WITH
              	"jaffle_shop_raw_raw_stores" as (SELECT CAST(1 AS VARCHAR) AS ID, CAST('Vice City' AS VARCHAR) AS NAME, CAST('2016-09-01T00:00:00' AS TIMESTAMP_NTZ(9)) AS OPENED_AT, CAST(0.2 AS FLOAT) AS TAX_RATE
UNION ALL
SELECT CAST(2 AS VARCHAR) AS ID, CAST('San Andreas' AS VARCHAR) AS NAME, CAST('2079-10-27T23:59:59.9999' AS TIMESTAMP_NTZ(9)) AS OPENED_AT, CAST(0.1 AS FLOAT) AS TAX_RATE),
  	"analytics_dbt_ksoenandar_stg_locations_expect" as (SELECT CAST(1 AS VARCHAR) AS LOCATION_ID, CAST('Vice City' AS VARCHAR) AS LOCATION_NAME, CAST(0.2 AS FLOAT) AS TAX_RATE, CAST('2016-09-01' AS TIMESTAMP_NTZ(9)) AS OPENED_DATE
UNION ALL
SELECT CAST(2 AS VARCHAR) AS LOCATION_ID, CAST('San Andreas' AS VARCHAR) AS LOCATION_NAME, CAST(0.1 AS FLOAT) AS TAX_RATE, CAST('2079-10-27' AS TIMESTAMP_NTZ(9)) AS OPENED_DATE),
  	"analytics_dbt_ksoenandar_stg_locations_actual" as (with

source as (

    select * from "jaffle_shop_raw_raw_stores"

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- text
        name as location_name,

        ---------- numerics
        tax_rate,

        ---------- timestamps
        date_trunc('day', opened_at) as opened_date

    from source

)

select * from renamed)
            (SELECT LOCATION_ID, LOCATION_NAME, OPENED_DATE, TAX_RATE, 'actual' AS actual_or_expected FROM "analytics_dbt_ksoenandar_stg_locations_actual")
            UNION ALL
            (SELECT LOCATION_ID, LOCATION_NAME, OPENED_DATE, TAX_RATE, 'expected' AS actual_or_expected FROM "analytics_dbt_ksoenandar_stg_locations_expect")
            ORDER BY LOCATION_ID, LOCATION_NAME, OPENED_DATE, TAX_RATE