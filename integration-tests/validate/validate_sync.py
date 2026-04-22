#!/usr/bin/env python3
"""
Validates that HMS tables synced to GDC are correct and readable by Athena.

Usage:
    python validate_sync.py --database integ_test_db --s3-output s3://bucket/athena-results/
"""

import argparse
import json
import sys
import time

import boto3


class SyncValidator:
    def __init__(self, database: str, athena_output: str, region: str = "us-east-1"):
        self.database = database
        self.athena_output = athena_output
        self.glue = boto3.client("glue", region_name=region)
        self.athena = boto3.client("athena", region_name=region)
        self.failures = []
        self.passes = []

    def run_all(self):
        """Run all validation checks."""
        self.validate_hive_tables()
        self.validate_iceberg_tables()
        self.validate_drop_table()
        self.validate_alter_table()
        self.print_report()
        return len(self.failures) == 0

    # ── Hive table validations ──────────────────────────────────────────

    def validate_hive_tables(self):
        for table_name, expected_format in [
            ("parquet_partitioned", "PARQUET"),
            ("orc_table", "ORC"),
            ("csv_table", "TEXTFILE"),
        ]:
            self._validate_table_exists(table_name)
            self._validate_athena_readable(table_name)
            self._validate_storage_format(table_name, expected_format)

        # CSV SerDe params
        self._validate_serde_params(
            "csv_table",
            expected_lib="org.apache.hadoop.hive.serde2.OpenCSVSerde",
            expected_params={"separatorChar": ",", "quoteChar": '"'},
        )

    # ── Iceberg table validations ───────────────────────────────────────

    def validate_iceberg_tables(self):
        iceberg_tables = [
            "iceberg_unpartitioned",
            "iceberg_partitioned",
            "iceberg_evolve",
            "iceberg_snapshots",
        ]
        for table_name in iceberg_tables:
            self._validate_table_exists(table_name)
            self._validate_iceberg_properties(table_name)
            self._validate_athena_readable(table_name)

        # Schema evolution: iceberg_evolve should have 3 columns after ALTER
        self._validate_iceberg_schema_evolution()

    def _validate_iceberg_properties(self, table_name: str):
        """Verify Iceberg-specific GDC properties."""
        test_id = f"iceberg_properties:{table_name}"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name=table_name)
            params = resp["Table"].get("Parameters", {})

            # table_type must be ICEBERG
            if params.get("table_type") != "ICEBERG":
                self._fail(test_id, f"table_type is '{params.get('table_type')}', expected 'ICEBERG'")
                return

            # metadata_location must exist and start with s3://
            meta_loc = params.get("metadata_location", "")
            if not meta_loc:
                self._fail(test_id, "metadata_location is missing")
                return
            if not meta_loc.startswith("s3://"):
                self._fail(test_id, f"metadata_location starts with '{meta_loc[:10]}', expected 's3://'")
                return

            self._pass(test_id)
        except Exception as e:
            self._fail(test_id, str(e))

    def _validate_iceberg_schema_evolution(self):
        """Verify iceberg_evolve has the evolved schema (3 columns)."""
        test_id = "iceberg_schema_evolution:iceberg_evolve"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name="iceberg_evolve")
            # For Iceberg, Athena reads schema from metadata, but we check GDC entry too
            # The key validation is that Athena can read the evolved schema
            result = self._run_athena_query(
                f"SELECT id, name, score FROM {self.database}.iceberg_evolve WHERE id = 2"
            )
            if result is None:
                self._fail(test_id, "Athena query failed for evolved schema")
                return
            self._pass(test_id)
        except Exception as e:
            self._fail(test_id, str(e))

    # ── Alter table validations ─────────────────────────────────────────

    def validate_alter_table(self):
        # 1.4: Added columns should appear
        test_id = "alter_add_columns:parquet_partitioned"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name="parquet_partitioned")
            col_names = [c["Name"] for c in resp["Table"]["StorageDescriptor"]["Columns"]]
            if "category" in col_names and "score" in col_names:
                self._pass(test_id)
            else:
                self._fail(test_id, f"Expected 'category' and 'score' in columns, got {col_names}")
        except Exception as e:
            self._fail(test_id, str(e))

        # 1.5: Custom properties should appear
        test_id = "alter_properties:orc_table"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name="orc_table")
            params = resp["Table"].get("Parameters", {})
            if params.get("custom.property") == "test_value":
                self._pass(test_id)
            else:
                self._fail(test_id, f"custom.property not found or wrong value: {params}")
        except Exception as e:
            self._fail(test_id, str(e))

    # ── Drop table validation ───────────────────────────────────────────

    def validate_drop_table(self):
        test_id = "drop_table:to_be_dropped"
        try:
            self.glue.get_table(DatabaseName=self.database, Name="to_be_dropped")
            self._fail(test_id, "Table still exists in GDC after DROP")
        except self.glue.exceptions.EntityNotFoundException:
            self._pass(test_id)
        except Exception as e:
            self._fail(test_id, str(e))

    # ── Helpers ─────────────────────────────────────────────────────────

    def _validate_table_exists(self, table_name: str):
        test_id = f"table_exists:{table_name}"
        try:
            self.glue.get_table(DatabaseName=self.database, Name=table_name)
            self._pass(test_id)
        except Exception as e:
            self._fail(test_id, str(e))

    def _validate_athena_readable(self, table_name: str):
        test_id = f"athena_readable:{table_name}"
        result = self._run_athena_query(f"SELECT * FROM {self.database}.{table_name} LIMIT 5")
        if result is not None:
            self._pass(test_id)
        else:
            self._fail(test_id, "Athena query failed or returned error")

    def _validate_storage_format(self, table_name: str, expected_keyword: str):
        test_id = f"storage_format:{table_name}"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name=table_name)
            sd = resp["Table"]["StorageDescriptor"]
            input_fmt = sd.get("InputFormat", "")
            output_fmt = sd.get("OutputFormat", "")
            combined = (input_fmt + output_fmt).lower()

            # Map format keywords to known class name fragments
            format_aliases = {
                "textfile": ["textinputformat", "textoutputformat", "ignorekeytextoutput"],
                "parquet": ["parquet"],
                "orc": ["orc"],
            }
            keywords = format_aliases.get(expected_keyword.lower(), [expected_keyword.lower()])
            if any(kw in combined for kw in keywords):
                self._pass(test_id)
            else:
                self._fail(test_id, f"Expected '{expected_keyword}' in formats, got {input_fmt} / {output_fmt}")
        except Exception as e:
            self._fail(test_id, str(e))

    def _validate_serde_params(self, table_name: str, expected_lib: str, expected_params: dict):
        test_id = f"serde_params:{table_name}"
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name=table_name)
            serde = resp["Table"]["StorageDescriptor"].get("SerdeInfo", {})
            lib = serde.get("SerializationLibrary", "")
            params = serde.get("Parameters", {})

            if lib != expected_lib:
                self._fail(test_id, f"SerDe lib is '{lib}', expected '{expected_lib}'")
                return
            for k, v in expected_params.items():
                if params.get(k) != v:
                    self._fail(test_id, f"SerDe param '{k}' is '{params.get(k)}', expected '{v}'")
                    return
            self._pass(test_id)
        except Exception as e:
            self._fail(test_id, str(e))

    def _run_athena_query(self, query: str, timeout_seconds: int = 120):
        """Execute an Athena query and wait for results."""
        try:
            exec_resp = self.athena.start_query_execution(
                QueryString=query,
                ResultConfiguration={"OutputLocation": self.athena_output},
            )
            exec_id = exec_resp["QueryExecutionId"]

            deadline = time.time() + timeout_seconds
            while time.time() < deadline:
                status = self.athena.get_query_execution(QueryExecutionId=exec_id)
                state = status["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    return self.athena.get_query_results(QueryExecutionId=exec_id)
                if state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
                    print(f"  Athena query failed: {reason}", file=sys.stderr)
                    return None
                time.sleep(2)

            print(f"  Athena query timed out after {timeout_seconds}s", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Athena error: {e}", file=sys.stderr)
            return None

    def _pass(self, test_id: str):
        self.passes.append(test_id)
        print(f"  ✓ {test_id}")

    def _fail(self, test_id: str, reason: str):
        self.failures.append((test_id, reason))
        print(f"  ✗ {test_id}: {reason}")

    def print_report(self):
        total = len(self.passes) + len(self.failures)
        print(f"\n{'='*60}")
        print(f"Results: {len(self.passes)}/{total} passed, {len(self.failures)} failed")
        if self.failures:
            print("\nFailures:")
            for test_id, reason in self.failures:
                print(f"  - {test_id}: {reason}")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Validate HMS-to-GDC sync")
    parser.add_argument("--database", default="integ_test_db")
    parser.add_argument("--s3-output", required=True, help="S3 path for Athena query results")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    validator = SyncValidator(args.database, args.s3_output, args.region)
    success = validator.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
