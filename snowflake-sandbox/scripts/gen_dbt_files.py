#!/usr/bin/env python

import subprocess
import json
import os
import logging
import errno
import yaml
import argparse


# TODO add ability to define alias for sources

STAGING_FOLDER = "staging"
SOURCE_FILE_NAME = "_sources.yml"
# staging model is in the form of <PREFIX><source_name><SEPARATOR_STAGING><table_name>.sql
PREFIX = "stg_"
SEPARATOR_STAGING = "__"

logging.basicConfig(level=logging.INFO)


# Setup of the different commands and their arguments
parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(
    title="subcommands", description="valid subcommands", dest="command"
)
generate_source = subparsers.add_parser("generate_source")
generate_staging = subparsers.add_parser("generate_staging")
generate_source_staging = subparsers.add_parser("generate_source_staging")

generate_source.add_argument("database_name")
generate_source.add_argument("schema_name")
generate_source.add_argument("--overwrite", action="store_true")
generate_source.add_argument("--name", type=str, help="Name for the source (default: schema_name)")
generate_source.add_argument("--table-names", type=str, help="Comma-separated list of table names (e.g. raw_orders,raw_customers)")
generate_source.add_argument("--include-descriptions", action="store_true")
generate_source.add_argument("--generate-columns", action="store_true")
generate_source.add_argument("--metadata-schema", type=str, help="Schema for metadata tables (default: schema_name)")
generate_source.add_argument("--metadata-database", type=str, help="Database for metadata tables (default: database_name)")
generate_source.add_argument("--metadata-table-prefix", type=str, default="", help="Prefix for metadata table names (default: macro default)")
generate_source.add_argument("--metadata-table-suffix", type=str, default="_metadata", help="Suffix for metadata table names (default: _metadata)")
generate_source.add_argument("--metadata-column-name", type=str, default="column_name", help="Metadata table column for column names (default: column_name)")
generate_source.add_argument("--metadata-description-column", type=str, default="column_description", help="Metadata table column for descriptions (default: column_description)")

generate_staging.add_argument("source_name")
generate_staging.add_argument("--overwrite", action="store_true")

generate_source_staging.add_argument("database_name")
generate_source_staging.add_argument("schema_name")
generate_source_staging.add_argument("--overwrite", action="store_true")
args = parser.parse_args()


def generate_sql_source(source_name, table_name):

    outp = (
        subprocess.run(
            [
                "dbt",
                "run-operation",
                "generate_base_model",
                "--args",
                f'{{"source_name": "{source_name}", "table_name": "{table_name}"}}',
                "--quiet",
            ],
            capture_output=True,
            text=True,
        )
        .stdout.strip()
    )

    return outp


def save_sql_source(source_name, table_name, overwrite=False):

    filename = f"{PREFIX}{source_name}{SEPARATOR_STAGING}{table_name}.sql"
    filepath = f"./models/{STAGING_FOLDER}/{source_name}/{filename}"

    if os.path.exists(filepath):
        if not overwrite:
            logging.info(f"The file {filename} already exists and was kept unchanged")
            return
        else:
            logging.warning(
                f"The file {filename} already existed and will be overwritten"
            )

    sql = generate_sql_source(source_name, table_name)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write(sql)
    logging.info(f"The file {filename} has been written to:")
    logging.info(f"{filepath}")


def generate_yml_sources(macro_args):
    """Run dbt update_source_metadata with the given args. Pass None for params to use macro defaults."""
    result = subprocess.run(
        [
            "dbt",
            "run-operation",
            "update_source_metadata",
            "--args",
            json.dumps(macro_args),
            "--quiet",
        ],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )

    # dbt may send "Created invocation id" to stdout and macro output (YAML) to stderr
    combined = (result.stdout or "") + (result.stderr or "")
    # Extract YAML content (starts with "version: 2"); ignore invocation/status lines
    if "version: 2" in combined:
        outp = combined[combined.index("version: 2"):].strip()
    else:
        outp = combined.strip()

    return outp


def save_yml_sources(macro_args, overwrite=False):
    schema_name = macro_args["schema_name"]
    database_name = macro_args["database_name"]
    filepath = f"./models/{STAGING_FOLDER}/{database_name}/{SOURCE_FILE_NAME}"

    if os.path.exists(filepath):
        if not overwrite:
            logging.info(
                f"The file {schema_name}/{SOURCE_FILE_NAME} already exists and was kept unchanged"
            )
            return
        else:
            logging.warning(
                f"The file {schema_name}/{SOURCE_FILE_NAME} already existed and will be overwritten"
            )

    yml_data = generate_yml_sources(macro_args)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write(yml_data)
    logging.info(f"The file {schema_name}/{SOURCE_FILE_NAME} has been written to:")
    logging.info(f"{filepath}")


def read_yml_sources(folder):
    """Read source definitions from models/staging/{folder}/_sources.yml.
    Use database_name for generate_source_staging, source_name for generate_staging."""
    yml_file_path = f"./models/{STAGING_FOLDER}/{folder}/{SOURCE_FILE_NAME}"
    if not os.path.exists(yml_file_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), yml_file_path)

    source_tables_list = []

    with open(yml_file_path) as yml_file:
        sources_list = yaml.load(yml_file, Loader=yaml.FullLoader)

    for source_table in sources_list["sources"][0]["tables"]:
        source_tables_list.append(
            {
                "source_name": sources_list["sources"][0]["name"],
                "table_name": source_table["name"],
            }
        )

    logging.info(
        f"Read {len(source_tables_list)} tables from {folder}/{SOURCE_FILE_NAME}"
    )
    return source_tables_list


if __name__ == "__main__":

    if args.command == "generate_source_staging":
        table_names = None
        if hasattr(args, "table_names") and args.table_names:
            table_names = [t.strip() for t in args.table_names.split(",")]
        macro_args = {
            "schema_name": args.schema_name,
            "database_name": args.database_name,
            "table_names": table_names,
            "name": getattr(args, "name", args.schema_name),
            "include_descriptions": getattr(args, "include_descriptions", False),
            "generate_columns": getattr(args, "generate_columns", False),
            "metadata_schema": getattr(args, "metadata_schema", None),
            "metadata_database": getattr(args, "metadata_database", None),
            "metadata_table_prefix": getattr(args, "metadata_table_prefix", ""),
            "metadata_table_suffix": getattr(args, "metadata_table_suffix", "_metadata"),
            "metadata_column_name_column": getattr(args, "metadata_column_name", "column_name"),
            "metadata_description_column": getattr(args, "metadata_description_column", "column_description"),
        }
        save_yml_sources(macro_args, overwrite=args.overwrite)
        source_tables = read_yml_sources(args.database_name)  # file is at models/staging/{database_name}/

        for source_table in source_tables:
            save_sql_source(
                source_table["source_name"], source_table["table_name"], args.overwrite
            )

    elif args.command == "generate_source":
        table_names = None
        if hasattr(args, "table_names") and args.table_names:
            table_names = [t.strip() for t in args.table_names.split(",")]
        macro_args = {
            "schema_name": args.schema_name,
            "database_name": args.database_name,
            "table_names": table_names,
            "name": getattr(args, "name", args.schema_name),
            "include_descriptions": getattr(args, "include_descriptions", False),
            "generate_columns": getattr(args, "generate_columns", False),
            "metadata_schema": getattr(args, "metadata_schema", None),
            "metadata_database": getattr(args, "metadata_database", None),
            "metadata_table_prefix": getattr(args, "metadata_table_prefix", ""),
            "metadata_table_suffix": getattr(args, "metadata_table_suffix", "_metadata"),
            "metadata_column_name_column": getattr(args, "metadata_column_name", "column_name"),
            "metadata_description_column": getattr(args, "metadata_description_column", "column_description"),
        }
        save_yml_sources(macro_args, overwrite=args.overwrite)

    elif args.command == "generate_staging":
        source_tables = read_yml_sources(args.source_name)

        for source_table in source_tables:
            save_sql_source(
                source_table["source_name"], source_table["table_name"], args.overwrite
            )