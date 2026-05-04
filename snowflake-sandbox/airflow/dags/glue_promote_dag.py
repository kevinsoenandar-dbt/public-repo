"""
DQ-gated promotion DAG: Glue Iceberg metadata pointer swap.

Implements the five-step pattern from the recommendation doc:

  1. Pre-flight       (OMS readiness + source-count register)   [stub]
  2. Trigger dbt build  (via DbtCloudRunJobOperator)
  3. Post DQ to OMS    (read run_results / Elementary, post)    [stub]
  4. Promote           (boto3 Glue metadata pointer swap)
  5. Post-load assure  (target count vs DQ count, register)     [stub]

Steps 1, 3, 5 are stubs in the sandbox -- ready to be replaced with
real OMS integration when wired up to CBA's environment.

Configuration is read from Airflow Variables and Connections so nothing
about CBA's environment is hard-coded into the DAG file.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# The plugins directory must be on PYTHONPATH (Airflow does this by
# default when plugins live under $AIRFLOW_HOME/plugins, or set
# AIRFLOW_HOME/plugins on PYTHONPATH for local testing).
from glue_promote import promote_all  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration (overridable via Airflow Variables)
# ---------------------------------------------------------------------------

# dbt Cloud
# `dbt_cloud_default` connection should be configured in Airflow with
# the dbt Cloud API token, account ID, and base URL.
DBT_CLOUD_CONN_ID = "dbt_cloud_default"

# The dbt Cloud job that runs `dbt build --target staging` (must be
# created in dbt Cloud first; see SETUP.md).
def _dbt_cloud_account_id() -> int:
    return int(Variable.get("dbt_cloud_account_id", default_var="127732"))


def _dbt_cloud_job_id() -> int:
    """ID of the dbt Cloud job that builds to the staging target.

    Set the `dbt_cloud_build_staging_job_id` Airflow Variable to the
    job's numeric ID.
    """
    return int(Variable.get("dbt_cloud_build_staging_job_id"))


# AWS / Glue
def _glue_catalog_id() -> str:
    """AWS account ID (used as Glue Catalog ID)."""
    return Variable.get("glue_catalog_id")


def _aws_region() -> str:
    return Variable.get("aws_region", default_var="ap-southeast-2")


def _staging_db() -> str:
    return Variable.get("glue_staging_database", default_var="dbt_ksoenandar_staging")


def _prod_db() -> str:
    return Variable.get("glue_prod_database", default_var="dbt_ksoenandar_prod")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def pre_flight_oms(**context) -> None:
    """STUB: pre-flight OMS readiness + source-count register.

    Replace with a real Python task that:
      1. Calls OMS for upstream ETL readiness
      2. For each source: count vs. expected, register the metric
      3. Aborts the DAG run on mismatch
    """
    logger.info("STUB pre_flight_oms: would call OMS readiness + count check here")


def post_dq_to_oms(**context) -> None:
    """STUB: post DQ outcomes to OMS.

    Replace with a real Python task that:
      1. Reads dbt's run_results.json from S3 (uploaded by the dbt Cloud job)
         -- or queries Elementary's metadata schema in Snowflake
      2. POSTs each test outcome (PASS / WARNING / FAIL with breach %) to OMS
    """
    logger.info("STUB post_dq_to_oms: would read dbt artefacts and post to OMS here")


def promote(**context) -> list[dict]:
    """Swap Glue metadata pointers from staging to prod.

    The actual DQ gate is upstream: this task only runs if every
    preceding task (including the dbt build, which halts on test
    failure) has succeeded. By default Airflow's trigger_rule is
    `all_success`, which is what we want here.
    """
    catalog_id = _glue_catalog_id()
    staging_db = _staging_db()
    prod_db = _prod_db()
    region = _aws_region()

    # Optional table filter via DAG params (e.g. for re-promoting a
    # subset after a partial failure)
    table_filter = context.get("params", {}).get("table_filter")

    logger.info(
        "Promoting Iceberg tables: catalog=%s staging=%s prod=%s region=%s filter=%s",
        catalog_id, staging_db, prod_db, region, table_filter,
    )

    results = promote_all(
        catalog_id=catalog_id,
        staging_db=staging_db,
        prod_db=prod_db,
        region_name=region,
        table_filter=table_filter,
    )

    swapped = sum(1 for r in results if r["swapped"])
    logger.info("Promotion complete: %d/%d tables swapped", swapped, len(results))

    # Push to XCom so downstream tasks can use the result
    return results


def post_load_assurance(**context) -> None:
    """STUB: post-load count check + register to OMS.

    Replace with a real Python task that:
      1. For each promoted table, count rows in the prod target
         filtered by etl_id + business_date
      2. Compare against the staging count produced by the dbt build
      3. Register success/fail with OMS
    """
    logger.info("STUB post_load_assurance: would verify counts and register OMS here")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="glue_promote",
    description="DQ-gated Iceberg promotion via Glue metadata pointer swap",
    schedule=None,                       # manually triggered for the sandbox
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["glue", "iceberg", "promotion", "sandbox"],
    params={
        # Optional: list of table names to restrict the promotion to.
        # If None, all Iceberg tables in the staging database are promoted.
        "table_filter": None,
    },
) as dag:

    pre_flight = PythonOperator(
        task_id="pre_flight_oms",
        python_callable=pre_flight_oms,
    )

    trigger_dbt_build = DbtCloudRunJobOperator(
        task_id="trigger_dbt_build",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        account_id=_dbt_cloud_account_id(),
        job_id=_dbt_cloud_job_id(),
        check_interval=30,
        timeout=3600,
        wait_for_termination=True,       # block until job completes (or fails)
    )

    post_dq = PythonOperator(
        task_id="post_dq_to_oms",
        python_callable=post_dq_to_oms,
    )

    promote_task = PythonOperator(
        task_id="promote_to_prod",
        python_callable=promote,
    )

    post_load = PythonOperator(
        task_id="post_load_assurance",
        python_callable=post_load_assurance,
    )

    # Sequence: 1 -> 2 -> 3 -> 4 -> 5
    pre_flight >> trigger_dbt_build >> post_dq >> promote_task >> post_load
