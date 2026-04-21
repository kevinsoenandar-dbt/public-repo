 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "ANALYTICS"."DBT_KSOENANDAR"."MARKET_DATA";
    -- dbt seed --
    
            insert into analytics.dbt_ksoenandar.market_data (REPORT_MONTH, FOOD_SALES, BEVERAGE_SALES, TOTAL_SALES) values
            (%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s)
        

;
  