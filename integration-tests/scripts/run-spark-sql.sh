#!/bin/bash
set -euo pipefail

# Wrapper script to run a SQL file from S3 via spark-sql.
# spark-sql -f doesn't support S3 paths directly, so we download first.
#
# Usage: run-spark-sql.sh <s3-sql-path>

S3_SQL_PATH="${1:?Usage: run-spark-sql.sh <s3-sql-path>}"
LOCAL_SQL="/tmp/$(basename ${S3_SQL_PATH})"

echo "Downloading SQL from ${S3_SQL_PATH}..."
aws s3 cp "${S3_SQL_PATH}" "${LOCAL_SQL}"

echo "Running spark-sql -f ${LOCAL_SQL}..."
spark-sql \
    --conf "spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.hive_prod.type=hive" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    -f "${LOCAL_SQL}"

echo "Done."
