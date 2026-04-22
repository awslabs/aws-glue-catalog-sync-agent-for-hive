#!/usr/bin/env python3
"""
Load test DDL generator for Hive Glue Catalog Sync Agent.

Generates parameterized Spark SQL DDL scripts that create high-volume HMS events
for testing sync agent correctness and performance under various workload patterns.

Usage:
    python3 generate-load.py --scenario <name> --output-dir <dir> --s3-bucket <bucket>
           [--databases N] [--tables-per-db N] [--partitions-per-table N]

Scenarios: burst-create, partition-heavy, multi-db, mixed-ops, sustained
"""

import argparse
import os
import sys

SCENARIOS = {
    "burst-create": {
        "databases": 1,
        "tables_per_db": 100,
        "partitions_per_table": 0,
        "operations": ["create"],
        "description": "CreateTable throughput, batch merging",
    },
    "partition-heavy": {
        "databases": 1,
        "tables_per_db": 5,
        "partitions_per_table": 200,
        "operations": ["create"],
        "description": "BatchCreatePartition batching, Glue API limits",
    },
    "multi-db": {
        "databases": 10,
        "tables_per_db": 10,
        "partitions_per_table": 10,
        "operations": ["create"],
        "description": "CreateDatabase + cross-DB parallelism",
    },
    "mixed-ops": {
        "databases": 1,
        "tables_per_db": 50,
        "partitions_per_table": 20,
        "operations": ["create", "alter", "drop"],
        "description": "Create -> Alter -> Drop interleaving",
    },
    "sustained": {
        "databases": 1,
        "tables_per_db": 500,
        "partitions_per_table": 5,
        "operations": ["create"],
        "description": "Queue depth, memory, long-running stability",
    },
}


def generate_ddl(scenario_config, s3_bucket):
    """Generate Spark SQL DDL statements for a given scenario configuration.

    Args:
        scenario_config: Dict with keys 'databases', 'tables_per_db',
                         'partitions_per_table', and 'operations'.
        s3_bucket: S3 bucket name for LOCATION clauses.

    Returns:
        List of SQL statement strings.
    """
    databases = scenario_config["databases"]
    tables_per_db = scenario_config["tables_per_db"]
    partitions_per_table = scenario_config["partitions_per_table"]
    operations = scenario_config.get("operations", ["create"])

    statements = []

    for db_idx in range(databases):
        db_name = f"load_test_db_{db_idx}"
        statements.append(f"CREATE DATABASE IF NOT EXISTS {db_name};")

        for tbl_idx in range(tables_per_db):
            tbl_name = f"load_test_tbl_{tbl_idx}"
            fq_table = f"{db_name}.{tbl_name}"
            location = f"s3://{s3_bucket}/load-test/{db_name}/{tbl_name}/"

            if partitions_per_table > 0:
                statements.append(
                    f"CREATE EXTERNAL TABLE {fq_table} "
                    f"(id INT, name STRING, value DOUBLE) "
                    f"PARTITIONED BY (dt STRING) "
                    f"STORED AS PARQUET "
                    f"LOCATION '{location}';"
                )
            else:
                statements.append(
                    f"CREATE EXTERNAL TABLE {fq_table} "
                    f"(id INT, name STRING, value DOUBLE) "
                    f"STORED AS PARQUET "
                    f"LOCATION '{location}';"
                )

            # Add partitions
            for p_idx in range(partitions_per_table):
                partition_val = f"2025-01-{p_idx + 1:02d}"
                statements.append(
                    f"ALTER TABLE {fq_table} ADD PARTITION (dt='{partition_val}') "
                    f"LOCATION '{location}dt={partition_val}/';"
                )

            # Mixed-ops: alter and drop a subset of tables
            if "alter" in operations:
                statements.append(
                    f"ALTER TABLE {fq_table} ADD COLUMNS (extra_col STRING);"
                )

            if "drop" in operations and tbl_idx % 5 == 0:
                statements.append(f"DROP TABLE IF EXISTS {fq_table};")

    return statements


def write_sql_files(statements, output_dir, scenario_name):
    """Write SQL statements to files in the output directory.

    Splits into files of up to 500 statements each.
    """
    os.makedirs(output_dir, exist_ok=True)
    max_per_file = 500
    file_idx = 0

    for i in range(0, len(statements), max_per_file):
        chunk = statements[i : i + max_per_file]
        filename = f"{scenario_name}_{file_idx:03d}.sql"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as f:
            f.write(f"-- Load test DDL: {scenario_name} (file {file_idx})\n")
            f.write(f"-- Generated statements: {len(chunk)}\n\n")
            for stmt in chunk:
                f.write(stmt + "\n")
        file_idx += 1

    return file_idx


def main():
    parser = argparse.ArgumentParser(
        description="Generate Spark SQL DDL for load testing the Hive Glue Catalog Sync Agent"
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=list(SCENARIOS.keys()),
        help="Load test scenario name",
    )
    parser.add_argument(
        "--output-dir", required=True, help="Directory to write SQL files"
    )
    parser.add_argument(
        "--s3-bucket", required=True, help="S3 bucket for LOCATION clauses"
    )
    parser.add_argument(
        "--databases", type=int, default=None, help="Override number of databases"
    )
    parser.add_argument(
        "--tables-per-db", type=int, default=None, help="Override tables per database"
    )
    parser.add_argument(
        "--partitions-per-table",
        type=int,
        default=None,
        help="Override partitions per table",
    )

    args = parser.parse_args()

    config = dict(SCENARIOS[args.scenario])
    if args.databases is not None:
        config["databases"] = args.databases
    if args.tables_per_db is not None:
        config["tables_per_db"] = args.tables_per_db
    if args.partitions_per_table is not None:
        config["partitions_per_table"] = args.partitions_per_table

    print(f"Generating DDL for scenario: {args.scenario}")
    print(f"  Databases: {config['databases']}")
    print(f"  Tables per DB: {config['tables_per_db']}")
    print(f"  Partitions per table: {config['partitions_per_table']}")
    print(f"  Operations: {config['operations']}")
    print(f"  S3 bucket: {args.s3_bucket}")

    statements = generate_ddl(config, args.s3_bucket)
    num_files = write_sql_files(statements, args.output_dir, args.scenario)

    print(f"Generated {len(statements)} statements in {num_files} file(s)")
    print(f"Output directory: {args.output_dir}")


if __name__ == "__main__":
    main()
