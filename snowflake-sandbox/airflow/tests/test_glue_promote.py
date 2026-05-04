"""
Unit tests for glue_promote.py.

Uses moto to mock the Glue API. No real AWS credentials required.

Run with:
    pytest airflow/tests/
"""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Make the plugins module importable without an Airflow installation
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))

from glue_promote import (  # noqa: E402
    list_iceberg_tables,
    promote_all,
    rollback,
    swap_metadata_pointer,
)

CATALOG_ID = "123456789012"
STAGING_DB = "analytics_staging"
PROD_DB = "analytics_prod"
REGION = "ap-southeast-2"


def _create_iceberg_table(
    glue_client,
    db_name: str,
    table_name: str,
    metadata_location: str,
) -> None:
    glue_client.create_table(
        CatalogId=CATALOG_ID,
        DatabaseName=db_name,
        TableInput={
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "table_type": "ICEBERG",
                "metadata_location": metadata_location,
            },
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
                "Location": f"s3://test-bucket/{db_name}/{table_name}/",
            },
        },
    )


@pytest.fixture
def glue():
    with mock_aws():
        client = boto3.client("glue", region_name=REGION)
        client.create_database(
            CatalogId=CATALOG_ID,
            DatabaseInput={"Name": STAGING_DB},
        )
        client.create_database(
            CatalogId=CATALOG_ID,
            DatabaseInput={"Name": PROD_DB},
        )
        yield client


def test_swap_promotes_staging_metadata_to_prod(glue):
    _create_iceberg_table(
        glue, STAGING_DB, "fact_orders",
        "s3://bucket/staging/fact_orders/v3.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fact_orders",
        "s3://bucket/prod/fact_orders/v2.json",
    )

    result = swap_metadata_pointer(
        glue, CATALOG_ID, STAGING_DB, PROD_DB, "fact_orders"
    )

    assert result["swapped"] is True
    assert result["new_location"] == "s3://bucket/staging/fact_orders/v3.json"
    assert result["previous_location"] == "s3://bucket/prod/fact_orders/v2.json"

    prod_after = glue.get_table(
        CatalogId=CATALOG_ID,
        DatabaseName=PROD_DB,
        Name="fact_orders",
    )["Table"]
    assert prod_after["Parameters"]["metadata_location"] == \
        "s3://bucket/staging/fact_orders/v3.json"
    assert prod_after["Parameters"]["previous_metadata_location"] == \
        "s3://bucket/prod/fact_orders/v2.json"


def test_swap_is_noop_when_locations_match(glue):
    same = "s3://bucket/staging/fact_orders/v3.json"
    _create_iceberg_table(glue, STAGING_DB, "fact_orders", same)
    _create_iceberg_table(glue, PROD_DB, "fact_orders", same)

    result = swap_metadata_pointer(
        glue, CATALOG_ID, STAGING_DB, PROD_DB, "fact_orders"
    )

    assert result["swapped"] is False


def test_promote_all_skips_non_iceberg_tables(glue):
    _create_iceberg_table(
        glue, STAGING_DB, "fact_orders",
        "s3://bucket/staging/fact_orders/v3.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fact_orders",
        "s3://bucket/prod/fact_orders/v2.json",
    )
    # A non-Iceberg table that should be ignored
    glue.create_table(
        CatalogId=CATALOG_ID,
        DatabaseName=STAGING_DB,
        TableInput={
            "Name": "regular_table",
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {},  # no table_type
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
                "Location": "s3://bucket/x/",
            },
        },
    )

    results = promote_all(
        catalog_id=CATALOG_ID,
        staging_db=STAGING_DB,
        prod_db=PROD_DB,
        glue_client=glue,
    )

    assert len(results) == 1
    assert results[0]["table"] == "fact_orders"


def test_promote_all_with_table_filter(glue):
    _create_iceberg_table(
        glue, STAGING_DB, "fact_orders",
        "s3://bucket/staging/fact_orders/v3.json",
    )
    _create_iceberg_table(
        glue, STAGING_DB, "fact_returns",
        "s3://bucket/staging/fact_returns/v3.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fact_orders",
        "s3://bucket/prod/fact_orders/v2.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fact_returns",
        "s3://bucket/prod/fact_returns/v2.json",
    )

    results = promote_all(
        catalog_id=CATALOG_ID,
        staging_db=STAGING_DB,
        prod_db=PROD_DB,
        glue_client=glue,
        table_filter=["fact_orders"],
    )

    assert len(results) == 1
    assert results[0]["table"] == "fact_orders"


def test_rollback_restores_previous_location(glue):
    _create_iceberg_table(
        glue, STAGING_DB, "fact_orders",
        "s3://bucket/staging/fact_orders/v3.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fact_orders",
        "s3://bucket/prod/fact_orders/v2.json",
    )

    swap_metadata_pointer(glue, CATALOG_ID, STAGING_DB, PROD_DB, "fact_orders")

    result = rollback(
        catalog_id=CATALOG_ID,
        prod_db=PROD_DB,
        table_name="fact_orders",
        glue_client=glue,
    )

    assert result["rolled_back_to"] == "s3://bucket/prod/fact_orders/v2.json"

    prod_after = glue.get_table(
        CatalogId=CATALOG_ID,
        DatabaseName=PROD_DB,
        Name="fact_orders",
    )["Table"]
    assert prod_after["Parameters"]["metadata_location"] == \
        "s3://bucket/prod/fact_orders/v2.json"


def test_rollback_fails_when_no_previous_location(glue):
    _create_iceberg_table(
        glue, PROD_DB, "fact_orders",
        "s3://bucket/prod/fact_orders/v2.json",
    )

    with pytest.raises(RuntimeError, match="No previous_metadata_location"):
        rollback(
            catalog_id=CATALOG_ID,
            prod_db=PROD_DB,
            table_name="fact_orders",
            glue_client=glue,
        )


def test_list_iceberg_tables_filters_correctly(glue):
    _create_iceberg_table(glue, STAGING_DB, "fact_a", "s3://x/a")
    _create_iceberg_table(glue, STAGING_DB, "fact_b", "s3://x/b")

    tables = list_iceberg_tables(glue, CATALOG_ID, STAGING_DB)
    assert sorted(tables) == ["fact_a", "fact_b"]

    filtered = list_iceberg_tables(
        glue, CATALOG_ID, STAGING_DB, table_filter=["fact_a"]
    )
    assert filtered == ["fact_a"]


def test_promote_all_handles_empty_staging(glue):
    results = promote_all(
        catalog_id=CATALOG_ID,
        staging_db=STAGING_DB,
        prod_db=PROD_DB,
        glue_client=glue,
    )
    assert results == []


def test_swap_bootstraps_prod_when_table_does_not_exist(glue):
    """First run: prod has no entry yet, the swap should create it
    from the staging template with previous_metadata_location empty."""
    _create_iceberg_table(
        glue, STAGING_DB, "fct_orders",
        "s3://bucket/staging/fct_orders/v3.json",
    )
    # No corresponding entry in PROD_DB

    result = swap_metadata_pointer(
        glue, CATALOG_ID, STAGING_DB, PROD_DB, "fct_orders"
    )

    assert result["swapped"] is True
    assert result["bootstrapped"] is True
    assert result["new_location"] == "s3://bucket/staging/fct_orders/v3.json"
    assert result["previous_location"] is None

    # Confirm prod now has the table pointing at staging's metadata
    prod_after = glue.get_table(
        CatalogId=CATALOG_ID,
        DatabaseName=PROD_DB,
        Name="fct_orders",
    )["Table"]
    assert prod_after["Parameters"]["metadata_location"] == \
        "s3://bucket/staging/fct_orders/v3.json"
    assert prod_after["Parameters"]["previous_metadata_location"] == ""
    assert prod_after["Parameters"]["table_type"] == "ICEBERG"


def test_promote_all_with_string_table_filter(glue):
    """A bare-string filter should be normalised to a single-element list,
    not expanded by `set()` to a set of characters."""
    _create_iceberg_table(
        glue, STAGING_DB, "fct_order_items_v2",
        "s3://bucket/staging/fct/v3.json",
    )
    _create_iceberg_table(
        glue, PROD_DB, "fct_order_items_v2",
        "s3://bucket/prod/fct/v2.json",
    )

    results = promote_all(
        catalog_id=CATALOG_ID,
        staging_db=STAGING_DB,
        prod_db=PROD_DB,
        glue_client=glue,
        table_filter="fct_order_items_v2",   # bare string, not a list
    )

    assert len(results) == 1
    assert results[0]["table"] == "fct_order_items_v2"
    assert results[0]["swapped"] is True


def test_promote_all_bootstrap_via_full_flow(glue):
    """End-to-end first-run scenario through promote_all."""
    _create_iceberg_table(
        glue, STAGING_DB, "fct_first_run",
        "s3://bucket/staging/fct_first_run/v1.json",
    )

    results = promote_all(
        catalog_id=CATALOG_ID,
        staging_db=STAGING_DB,
        prod_db=PROD_DB,
        glue_client=glue,
    )

    assert len(results) == 1
    assert results[0]["bootstrapped"] is True
    assert results[0]["swapped"] is True

    prod_after = glue.get_table(
        CatalogId=CATALOG_ID,
        DatabaseName=PROD_DB,
        Name="fct_first_run",
    )["Table"]
    assert prod_after["Parameters"]["metadata_location"] == \
        "s3://bucket/staging/fct_first_run/v1.json"
