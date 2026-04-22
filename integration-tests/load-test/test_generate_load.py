"""
Property-based tests for the load test DDL generator.

Uses pytest + hypothesis to verify correctness properties of generate_ddl().
"""

import re
import sys
import os

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

# Ensure the load-test directory is importable
sys.path.insert(0, os.path.dirname(__file__))

# Import from generate-load.py (hyphenated filename requires importlib)
import importlib

generate_load = importlib.import_module("generate-load")
generate_ddl = generate_load.generate_ddl
SCENARIOS = generate_load.SCENARIOS


# --- Strategies ---

databases_st = st.integers(min_value=1, max_value=20)
tables_per_db_st = st.integers(min_value=1, max_value=100)
partitions_per_table_st = st.integers(min_value=0, max_value=50)

# S3 bucket names: lowercase letters, numbers, hyphens, 3-63 chars
bucket_name_st = st.from_regex(r"[a-z][a-z0-9\-]{2,30}[a-z0-9]", fullmatch=True)


# --- Property 9: DDL generation produces correct statement counts ---
# Validates: Requirements 5.1


class TestDDLStatementCounts:
    """Property 9: DDL generation produces correct statement counts.

    For any scenario parameters (databases D, tables-per-db T,
    partitions-per-table P), the generated SQL contains exactly D CREATE
    DATABASE statements, D*T CREATE TABLE statements, and the correct
    number of ALTER TABLE ADD PARTITION statements based on P.

    **Validates: Requirements 5.1**
    """

    @given(
        databases=databases_st,
        tables_per_db=tables_per_db_st,
        partitions_per_table=partitions_per_table_st,
    )
    @settings(max_examples=200)
    def test_create_only_statement_counts(
        self, databases, tables_per_db, partitions_per_table
    ):
        """For create-only scenarios, verify exact statement counts."""
        config = {
            "databases": databases,
            "tables_per_db": tables_per_db,
            "partitions_per_table": partitions_per_table,
            "operations": ["create"],
        }
        statements = generate_ddl(config, "test-bucket")

        create_db = [s for s in statements if s.startswith("CREATE DATABASE")]
        create_tbl = [s for s in statements if s.startswith("CREATE EXTERNAL TABLE")]
        add_partition = [s for s in statements if "ADD PARTITION" in s]

        assert len(create_db) == databases
        assert len(create_tbl) == databases * tables_per_db
        assert len(add_partition) == databases * tables_per_db * partitions_per_table

    @given(
        databases=databases_st,
        tables_per_db=tables_per_db_st,
        partitions_per_table=partitions_per_table_st,
    )
    @settings(max_examples=200)
    def test_mixed_ops_statement_counts(
        self, databases, tables_per_db, partitions_per_table
    ):
        """For mixed-ops scenarios, verify create + alter + drop counts."""
        config = {
            "databases": databases,
            "tables_per_db": tables_per_db,
            "partitions_per_table": partitions_per_table,
            "operations": ["create", "alter", "drop"],
        }
        statements = generate_ddl(config, "test-bucket")

        create_db = [s for s in statements if s.startswith("CREATE DATABASE")]
        create_tbl = [s for s in statements if s.startswith("CREATE EXTERNAL TABLE")]
        add_partition = [s for s in statements if "ADD PARTITION" in s and "ADD COLUMNS" not in s]
        alter_col = [s for s in statements if "ADD COLUMNS" in s]
        drop_tbl = [s for s in statements if s.startswith("DROP TABLE")]

        assert len(create_db) == databases
        assert len(create_tbl) == databases * tables_per_db
        assert len(add_partition) == databases * tables_per_db * partitions_per_table
        # Every table gets an ALTER ADD COLUMNS
        assert len(alter_col) == databases * tables_per_db
        # Every 5th table (index % 5 == 0) gets dropped
        expected_drops = sum(
            1
            for _ in range(databases)
            for t in range(tables_per_db)
            if t % 5 == 0
        )
        assert len(drop_tbl) == expected_drops


# --- Property 10: DDL generation parameterizes S3 bucket ---
# Validates: Requirements 5.4


class TestS3BucketParameterization:
    """Property 10: DDL generation parameterizes S3 bucket.

    For any S3 bucket name, all LOCATION clauses in the generated SQL
    contain that bucket name.

    **Validates: Requirements 5.4**
    """

    @given(
        bucket_name=bucket_name_st,
        databases=databases_st,
        tables_per_db=st.integers(min_value=1, max_value=20),
        partitions_per_table=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=200)
    def test_all_locations_contain_bucket(
        self, bucket_name, databases, tables_per_db, partitions_per_table
    ):
        """Every LOCATION clause must reference the provided S3 bucket."""
        config = {
            "databases": databases,
            "tables_per_db": tables_per_db,
            "partitions_per_table": partitions_per_table,
            "operations": ["create"],
        }
        statements = generate_ddl(config, bucket_name)

        location_stmts = [s for s in statements if "LOCATION" in s]
        # There should be at least one LOCATION (from CREATE TABLE)
        assert len(location_stmts) > 0

        for stmt in location_stmts:
            assert f"s3://{bucket_name}/" in stmt, (
                f"LOCATION clause missing bucket '{bucket_name}': {stmt}"
            )

    @given(bucket_name=bucket_name_st)
    @settings(max_examples=200)
    def test_no_hardcoded_bucket_in_locations(self, bucket_name):
        """No LOCATION clause should contain a bucket name other than the provided one."""
        config = {
            "databases": 2,
            "tables_per_db": 3,
            "partitions_per_table": 2,
            "operations": ["create"],
        }
        statements = generate_ddl(config, bucket_name)

        location_pattern = re.compile(r"LOCATION\s+'s3://([^/]+)/")
        for stmt in statements:
            match = location_pattern.search(stmt)
            if match:
                found_bucket = match.group(1)
                assert found_bucket == bucket_name, (
                    f"Expected bucket '{bucket_name}' but found '{found_bucket}'"
                )
