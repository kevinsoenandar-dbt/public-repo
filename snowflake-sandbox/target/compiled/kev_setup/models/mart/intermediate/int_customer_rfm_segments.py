import pandas as pd


def model(dbt, session):
    dbt.config(materialized="table")

    # --- Reference upstream SQL model ---
    order_items = dbt.ref("fct_order_items", v=2).to_pandas()

    # --- Compute RFM metrics per customer ---
    # Snowpark returns column names in uppercase after to_pandas()
    snapshot_date = order_items["ORDERED_AT"].max()

    rfm = (
        order_items
        .groupby("CUSTOMER_ID")
        .agg(
            recency_days=("ORDERED_AT", lambda x: (snapshot_date - x.max()).days),
            order_frequency=("ORDER_ID", "nunique"),
            lifetime_value=("PRODUCT_PRICE", "sum"),
        )
        .reset_index()
    )

    # --- Score each dimension into quintiles (1–5) using pd.qcut() ---
    # This is the key step that would require multiple NTILE() CTEs in SQL.
    # Recency: fewer days since last order = better, so labels are inverted [5..1]
    rfm["r_score"] = pd.qcut(rfm["recency_days"].rank(method="first"), q=5, labels=[5, 4, 3, 2, 1])
    rfm["f_score"] = pd.qcut(rfm["order_frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5])
    rfm["m_score"] = pd.qcut(rfm["lifetime_value"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5])

    rfm["rfm_score"] = (
        rfm["r_score"].astype(str)
        + rfm["f_score"].astype(str)
        + rfm["m_score"].astype(str)
    )

    # --- Map RFM scores to human-readable segments ---
    def assign_segment(row):
        r, f = int(row["r_score"]), int(row["f_score"])
        if r >= 4 and f >= 4:
            return "Champions"
        elif r >= 3 and f >= 3:
            return "Loyal Customers"
        elif r >= 3:
            return "Potential Loyalists"
        elif r >= 2:
            return "At-Risk"
        else:
            return "Hibernating"

    rfm["customer_segment"] = rfm.apply(assign_segment, axis=1)

    result = rfm.rename(columns={"CUSTOMER_ID": "customer_id"})[[
        "customer_id",
        "recency_days",
        "order_frequency",
        "lifetime_value",
        "rfm_score",
        "customer_segment",
    ]]
    result.columns = result.columns.str.upper()
    return result


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
    identifier = "int_customer_rfm_segments"
    
    def __repr__(self):
        return 'analytics.dbt_ksoenandar.int_customer_rfm_segments'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


