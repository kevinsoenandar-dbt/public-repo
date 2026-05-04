"""
Glue Iceberg metadata pointer swap for DQ-gated promotion.

Reads the current `metadata_location` from each Iceberg table in the
staging Glue database and updates the prod database's same-named entry
to match. Preserves `previous_metadata_location` for rollback.

Used as the promotion step in the Airflow DAG (see ../dags/glue_promote_dag.py).

This module is intentionally free of Airflow imports so it can be
unit-tested in isolation against moto-mocked Glue (see ../tests/).
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

import boto3

logger = logging.getLogger(__name__)


def list_iceberg_tables(
    glue_client,
    catalog_id: str,
    database_name: str,
    table_filter: Optional[Iterable[str]] = None,
) -> list[str]:
    """Enumerate Iceberg tables in a Glue database.

    Iceberg tables are identified by `Parameters.table_type == 'ICEBERG'`,
    which is the convention Iceberg engines (including Snowflake) use
    when registering tables in Glue.
    """
    paginator = glue_client.get_paginator("get_tables")
    filter_set = set(table_filter) if table_filter is not None else None
    tables: list[str] = []

    for page in paginator.paginate(
        CatalogId=catalog_id,
        DatabaseName=database_name,
    ):
        for table in page["TableList"]:
            params = table.get("Parameters", {}) or {}
            if params.get("table_type", "").upper() != "ICEBERG":
                continue
            if filter_set is not None and table["Name"] not in filter_set:
                continue
            tables.append(table["Name"])
    return tables


def swap_metadata_pointer(
    glue_client,
    catalog_id: str,
    staging_db: str,
    prod_db: str,
    table_name: str,
) -> dict:
    """Point prod's Glue table entry at staging's current Iceberg metadata.

    Returns a dict describing the swap, including the previous and new
    metadata locations. Useful for logging, audit trail, and rollback.
    """
    staging = glue_client.get_table(
        CatalogId=catalog_id,
        DatabaseName=staging_db,
        Name=table_name,
    )["Table"]

    prod = glue_client.get_table(
        CatalogId=catalog_id,
        DatabaseName=prod_db,
        Name=table_name,
    )["Table"]

    new_metadata_location = staging["Parameters"]["metadata_location"]
    current_prod_location = prod["Parameters"].get("metadata_location")

    if new_metadata_location == current_prod_location:
        logger.info(
            "No-op for %s.%s: staging and prod already match (%s)",
            prod_db,
            table_name,
            new_metadata_location,
        )
        return {
            "table": table_name,
            "previous_location": current_prod_location,
            "new_location": new_metadata_location,
            "swapped": False,
        }

    updated_parameters = {
        **prod["Parameters"],
        "metadata_location": new_metadata_location,
        "previous_metadata_location": current_prod_location or "",
        "table_type": "ICEBERG",
    }

    table_input = {
        "Name": prod["Name"],
        "TableType": prod["TableType"],
        "Parameters": updated_parameters,
        "StorageDescriptor": prod["StorageDescriptor"],
    }

    glue_client.update_table(
        CatalogId=catalog_id,
        DatabaseName=prod_db,
        TableInput=table_input,
        SkipArchive=False,  # retain TableVersion history for rollback
    )

    logger.info(
        "Swapped %s.%s: %s -> %s",
        prod_db,
        table_name,
        current_prod_location,
        new_metadata_location,
    )

    return {
        "table": table_name,
        "previous_location": current_prod_location,
        "new_location": new_metadata_location,
        "swapped": True,
    }


def promote_all(
    catalog_id: str,
    staging_db: str,
    prod_db: str,
    region_name: str = "ap-southeast-2",
    table_filter: Optional[Iterable[str]] = None,
    glue_client=None,
) -> list[dict]:
    """Promote every Iceberg table in `staging_db` to its `prod_db` counterpart.

    On any per-table failure, raises immediately. Tables already swapped
    in the same run are not rolled back -- the caller is responsible for
    deciding whether to invoke rollback() on previously-swapped tables.
    """
    if glue_client is None:
        glue_client = boto3.client("glue", region_name=region_name)

    tables = list_iceberg_tables(
        glue_client, catalog_id, staging_db, table_filter
    )

    if not tables:
        logger.warning("No Iceberg tables found in %s", staging_db)
        return []

    logger.info(
        "Promoting %d table(s) from %s -> %s: %s",
        len(tables),
        staging_db,
        prod_db,
        tables,
    )

    results: list[dict] = []
    for table_name in tables:
        try:
            result = swap_metadata_pointer(
                glue_client,
                catalog_id,
                staging_db,
                prod_db,
                table_name,
            )
            results.append(result)
        except Exception:
            logger.exception(
                "Failed to swap %s.%s -- aborting promotion run",
                prod_db,
                table_name,
            )
            raise

    return results


def rollback(
    catalog_id: str,
    prod_db: str,
    table_name: str,
    region_name: str = "ap-southeast-2",
    glue_client=None,
) -> dict:
    """Roll a single prod table back to its `previous_metadata_location`.

    Useful for emergency rollback after a bad promotion. Iceberg's
    `previous_metadata_location` is automatically populated by the
    swap, so this is a single Glue update.
    """
    if glue_client is None:
        glue_client = boto3.client("glue", region_name=region_name)

    prod = glue_client.get_table(
        CatalogId=catalog_id,
        DatabaseName=prod_db,
        Name=table_name,
    )["Table"]

    previous_location = prod["Parameters"].get("previous_metadata_location")
    if not previous_location:
        raise RuntimeError(
            f"No previous_metadata_location on {prod_db}.{table_name}; "
            f"cannot rollback"
        )

    current_location = prod["Parameters"]["metadata_location"]

    updated_parameters = {
        **prod["Parameters"],
        "metadata_location": previous_location,
        "previous_metadata_location": current_location,
        "table_type": "ICEBERG",
    }

    table_input = {
        "Name": prod["Name"],
        "TableType": prod["TableType"],
        "Parameters": updated_parameters,
        "StorageDescriptor": prod["StorageDescriptor"],
    }

    glue_client.update_table(
        CatalogId=catalog_id,
        DatabaseName=prod_db,
        TableInput=table_input,
        SkipArchive=False,
    )

    logger.info(
        "Rolled back %s.%s: %s -> %s",
        prod_db,
        table_name,
        current_location,
        previous_location,
    )

    return {
        "table": table_name,
        "rolled_back_to": previous_location,
        "previous_location_was": current_location,
    }
