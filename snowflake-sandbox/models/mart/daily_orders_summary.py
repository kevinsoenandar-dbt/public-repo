import holidays

def model(dbt, session):
    # dbt will install/import this package for the Python model runtime
    dbt.config(packages=["holidays"])

    from snowflake.snowpark.functions import col, lit, sum as sum_, to_date, when

    order_items = dbt.ref('fct_order_items', v=2)

    # IMPORTANT: you can't do `date_col in french_holidays` when date_col is a Snowpark Column.
    # Instead, generate a static python list of holiday dates and use Snowpark `.isin(...)`.
    au_holidays = holidays.Australia()
    holiday_dates = list(au_holidays.keys())

    daily = (
        order_items
        .with_column('order_date', to_date(col('ordered_at')))
        .with_column('is_holiday', col('order_date').isin(holiday_dates))
        .group_by(col('order_date'), col('is_holiday'))
        .agg(
            sum_(when(col('is_drink_item'), col('product_price')).otherwise(lit(0))).alias('drink_order_amount'),
            sum_(when(col('is_food_item'), col('product_price')).otherwise(lit(0))).alias('food_order_amount'),
        )
        .select('order_date', 'is_holiday', 'drink_order_amount', 'food_order_amount')
        .orderBy('order_date')
    )

    return daily
