with

base_dates as (
    


    


    with rawdata as (

        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
    
    

    )

    select *
    from unioned
    where generated_number <= 7305
    order by generated_number



    ),

    all_periods as (

        select (
            

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        DATE('2010-01-01')
        )


        ) as date_day
        from rawdata

    ),

    filtered as (

        select *
        from all_periods
        where date_day <= DATE('2030-01-01')

    )

    select * from filtered


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