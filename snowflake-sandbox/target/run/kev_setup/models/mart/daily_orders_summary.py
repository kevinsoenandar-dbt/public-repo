
  
    

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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"fct_order_items.v2": "analytics.dbt_ksoenandar.fct_order_items_v2"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}
meta_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "analytics"
    schema = "dbt_ksoenandar"
    identifier = "daily_orders_summary"
    
    def __repr__(self):
        return 'analytics.dbt_ksoenandar.daily_orders_summary'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------





def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
            session.use_database(target_relation.database)
            session.use_schema(target_relation.schema)
            # session.write_pandas does not have overwrite function
            df = session.createDataFrame(df)
    
    df.write.mode("overwrite").save_as_table('analytics.dbt_ksoenandar.daily_orders_summary', table_type='transient')


def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"


  