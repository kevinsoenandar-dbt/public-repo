with

base_dates as (
    
    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        DATE('2010-01-01')
        )


    ) as date_day
    from table(flatten(input => array_generate_range(0, datediff(
        day,
        DATE('2010-01-01'),
        DATE('2030-01-01')
        ) )))

),

final as (
    select
        cast(date_day as date) as date_day
    from base_dates
)

select *
from final
where date_day > dateadd(year, -20, current_date())  -- Keep recent dates only
  and date_day < dateadd(day, 30, current_date())