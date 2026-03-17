with all_dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2029-12-31' as date)"
    )
    }}
)

select 
    cast(date_day as date) as date_day

from all_dates
