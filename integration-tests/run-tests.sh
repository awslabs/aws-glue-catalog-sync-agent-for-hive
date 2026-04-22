#!/bin/bash
set -euo pipefail

# ─── Configuration ──────────────────────────────────────────────────────────
CLUSTER_ID="${EMR_CLUSTER_ID:?Set EMR_CLUSTER_ID}"
S3_BUCKET="${TEST_S3_BUCKET:?Set TEST_S3_BUCKET}"
S3_SCRIPTS="s3://${S3_BUCKET}/integ-test/scripts"
S3_ATHENA_OUTPUT="s3://${S3_BUCKET}/integ-test/athena-results/"
REGION="${AWS_REGION:-us-east-1}"
DATABASE="integ_test_db"
SYNC_WAIT_SECONDS="${SYNC_WAIT:-90}"  # Wait for sync agent batch window + processing

echo "=== HMS-to-GDC Sync Agent Integration Tests ==="
echo "Cluster: ${CLUSTER_ID}"
echo "S3 Bucket: ${S3_BUCKET}"
echo "Region: ${REGION}"
echo ""

# ─── Upload SQL scripts to S3 ──────────────────────────────────────────────
echo "Uploading EMR step scripts..."
for f in emr-steps/*.sql; do
    # Substitute S3 bucket placeholder
    sed "s|\${S3_BUCKET}|s3://${S3_BUCKET}|g" "$f" > "/tmp/$(basename $f)"
    aws s3 cp "/tmp/$(basename $f)" "${S3_SCRIPTS}/$(basename $f)" --region "${REGION}"
done

# ─── Submit EMR Steps ──────────────────────────────────────────────────────
submit_spark_sql_step() {
    local step_name="$1"
    local script_path="$2"

    echo "Submitting step: ${step_name}..."
    STEP_ID=$(aws emr add-steps \
        --cluster-id "${CLUSTER_ID}" \
        --region "${REGION}" \
        --steps "[{
            \"Type\": \"CUSTOM_JAR\",
            \"Name\": \"${step_name}\",
            \"ActionOnFailure\": \"CONTINUE\",
            \"Jar\": \"command-runner.jar\",
            \"Args\": [
                \"spark-sql\",
                \"--conf\", \"spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog\",
                \"--conf\", \"spark.sql.catalog.hive_prod.type=hive\",
                \"--conf\", \"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\",
                \"-f\", \"${script_path}\"
            ]
        }]" \
        --query 'StepIds[0]' --output text)

    echo "  Step ID: ${STEP_ID}"
    echo "  Waiting for completion..."
    aws emr wait step-complete \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --region "${REGION}"

    STATUS=$(aws emr describe-step \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --region "${REGION}" \
        --query 'Step.Status.State' --output text)

    if [ "${STATUS}" != "COMPLETED" ]; then
        echo "  ✗ Step failed with status: ${STATUS}"
        REASON=$(aws emr describe-step \
            --cluster-id "${CLUSTER_ID}" \
            --step-id "${STEP_ID}" \
            --region "${REGION}" \
            --query 'Step.Status.FailureDetails.Reason' --output text)
        echo "  Reason: ${REASON}"
        return 1
    fi
    echo "  ✓ Step completed"
}

# Step 1: Create Hive tables
submit_spark_sql_step "Create Hive Tables" "${S3_SCRIPTS}/01_create_hive_tables.sql"

# Step 2: Create Iceberg tables
submit_spark_sql_step "Create Iceberg Tables" "${S3_SCRIPTS}/02_create_iceberg_tables.sql"

# Step 3: Alter and drop operations
submit_spark_sql_step "Alter and Drop" "${S3_SCRIPTS}/03_alter_and_drop.sql"

# ─── Wait for sync agent to process ────────────────────────────────────────
echo ""
echo "Waiting ${SYNC_WAIT_SECONDS}s for sync agent to process events..."
sleep "${SYNC_WAIT_SECONDS}"

# Step 4: Drop table (after sync of create)
submit_spark_sql_step "Drop Table" "${S3_SCRIPTS}/04_drop_table.sql"

# Wait again for the drop to sync
echo "Waiting ${SYNC_WAIT_SECONDS}s for drop to sync..."
sleep "${SYNC_WAIT_SECONDS}"

# ─── Validate ──────────────────────────────────────────────────────────────
echo ""
echo "=== Running Validation ==="
python3 validate/validate_sync.py \
    --database "${DATABASE}" \
    --s3-output "${S3_ATHENA_OUTPUT}" \
    --region "${REGION}"

echo ""
echo "=== Integration Tests Complete ==="
