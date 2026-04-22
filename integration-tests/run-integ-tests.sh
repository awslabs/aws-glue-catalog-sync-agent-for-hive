#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Tee all output to a log file while keeping it on stdout/stderr
INTEG_LOG="${SCRIPT_DIR}/../target/integ-test-output.log"
mkdir -p "$(dirname "${INTEG_LOG}")" 2>/dev/null || true
exec > >(tee -a "${INTEG_LOG}") 2>&1

# ─── Full integration test lifecycle ────────────────────────────────────────
# Called by Maven exec-maven-plugin during the integration-test phase.
#
# Environment variables (required):
#   TEST_S3_BUCKET   - S3 bucket for test artifacts
#   SUBNET_ID        - VPC subnet for EMR cluster
#
# Optional:
#   AWS_REGION       - defaults to us-east-1
#   EMR_RELEASE      - defaults to emr-7.1.0
#   GLUE_CATALOG_ID  - defaults to empty (account default)
# ────────────────────────────────────────────────────────────────────────────

S3_BUCKET="${TEST_S3_BUCKET:?Set TEST_S3_BUCKET}"
SUBNET="${SUBNET_ID:?Set SUBNET_ID}"
REGION="${AWS_REGION:-us-east-1}"
EMR_RELEASE="${EMR_RELEASE:-emr-7.1.0}"
CATALOG_ID="${GLUE_CATALOG_ID:-}"
STACK_NAME="hms-gdc-sync-integ-$(date +%s)"
S3_JAR_KEY="integ-test/jars/HiveGlueCatalogSyncAgent-complete.jar"
S3_BOOTSTRAP_KEY="integ-test/scripts/bootstrap-install-agent.sh"
DATABASE="integ_test_db"
CLUSTER_ID=""

# Find the sync agent fat JAR — fail early with a clear message
SYNC_AGENT_JAR=""
for f in ../target/HiveGlueCatalogSyncAgent-*-complete.jar; do
    if [ -f "$f" ]; then
        SYNC_AGENT_JAR="$f"
        break
    fi
done
if [ -z "${SYNC_AGENT_JAR}" ]; then
    echo "ERROR: Sync agent fat JAR not found in ../target/"
    echo "Run 'mvn package assembly:assembly -DskipTests' first."
    exit 1
fi

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Delete test database from Glue catalog
    echo "Cleaning up Glue catalog..."
    TABLES=$(aws glue get-tables --database-name "${DATABASE}" --region "${REGION}" \
        --query 'TableList[].Name' --output text 2>/dev/null || true)
    for t in ${TABLES}; do
        aws glue delete-table --database-name "${DATABASE}" --name "${t}" --region "${REGION}" 2>/dev/null || true
    done
    aws glue delete-database --name "${DATABASE}" --region "${REGION}" 2>/dev/null || true

    # Delete CFN stack (terminates EMR cluster)
    if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" &>/dev/null; then
        echo "Deleting CloudFormation stack ${STACK_NAME}..."
        aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
        aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" --region "${REGION}" || true
    fi

    # Clean up S3 test data
    echo "Cleaning up S3 test data..."
    aws s3 rm "s3://${S3_BUCKET}/integ-test/" --recursive --region "${REGION}" 2>/dev/null || true

    echo "Cleanup complete."
}

trap cleanup EXIT

echo "============================================================"
echo "  HMS-to-GDC Sync Agent Integration Test"
echo "============================================================"
echo "Stack:   ${STACK_NAME}"
echo "Bucket:  ${S3_BUCKET}"
echo "Subnet:  ${SUBNET}"
echo "Region:  ${REGION}"
echo "Release: ${EMR_RELEASE}"
echo ""

# ─── 1. Upload artifacts to S3 ─────────────────────────────────────────────
echo "=== Uploading artifacts to S3 ==="

# Sync agent JAR
JAR_FILE="${SYNC_AGENT_JAR}"
echo "Uploading sync agent JAR: ${JAR_FILE}"
aws s3 cp "${JAR_FILE}" "s3://${S3_BUCKET}/${S3_JAR_KEY}" --region "${REGION}"

# Bootstrap script
echo "Uploading bootstrap script..."
aws s3 cp "${SCRIPT_DIR}/scripts/bootstrap-install-agent.sh" \
    "s3://${S3_BUCKET}/${S3_BOOTSTRAP_KEY}" --region "${REGION}"

# Spark SQL wrapper script
echo "Uploading spark-sql wrapper script..."
aws s3 cp "${SCRIPT_DIR}/scripts/run-spark-sql.sh" \
    "s3://${S3_BUCKET}/integ-test/scripts/run-spark-sql.sh" --region "${REGION}"

# SQL step scripts (with S3 bucket substitution)
echo "Uploading EMR step scripts..."
for f in "${SCRIPT_DIR}/emr-steps/"*.sql; do
    sed "s|\${S3_BUCKET}|s3://${S3_BUCKET}|g" "$f" > "/tmp/$(basename $f)"
    aws s3 cp "/tmp/$(basename $f)" \
        "s3://${S3_BUCKET}/integ-test/scripts/$(basename $f)" --region "${REGION}"
done

# ─── 2. Deploy CloudFormation stack ────────────────────────────────────────
echo ""
echo "=== Deploying EMR cluster via CloudFormation ==="

PARAMS="ParameterKey=S3Bucket,ParameterValue=${S3_BUCKET}"
PARAMS="${PARAMS} ParameterKey=SubnetId,ParameterValue=${SUBNET}"
PARAMS="${PARAMS} ParameterKey=EmrReleaseLabel,ParameterValue=${EMR_RELEASE}"
PARAMS="${PARAMS} ParameterKey=GlueCatalogRegion,ParameterValue=${REGION}"
PARAMS="${PARAMS} ParameterKey=SyncAgentJarS3Key,ParameterValue=${S3_JAR_KEY}"
if [ -n "${CATALOG_ID}" ]; then
    PARAMS="${PARAMS} ParameterKey=GlueCatalogId,ParameterValue=${CATALOG_ID}"
fi

aws cloudformation create-stack \
    --stack-name "${STACK_NAME}" \
    --template-body "file://${SCRIPT_DIR}/cfn/emr-integ-test.yaml" \
    --parameters ${PARAMS} \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}"

echo "Waiting for stack creation (this takes ~10 minutes)..."
if ! aws cloudformation wait stack-create-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" 2>&1; then
    echo ""
    echo "ERROR: CloudFormation stack creation failed. Events:"
    aws cloudformation describe-stack-events \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].{Resource:LogicalResourceId,Reason:ResourceStatusReason}' \
        --output table 2>&1 || true
    exit 1
fi

CLUSTER_ID=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`ClusterId`].OutputValue' \
    --output text)

echo "EMR Cluster ID: ${CLUSTER_ID}"

# Wait for cluster to be in WAITING state
echo "Waiting for cluster to be ready..."
aws emr wait cluster-running --cluster-id "${CLUSTER_ID}" --region "${REGION}"
echo "Cluster is ready."

# ─── 3. Submit EMR steps ───────────────────────────────────────────────────
echo ""
echo "=== Submitting EMR Steps ==="

S3_SCRIPTS="s3://${S3_BUCKET}/integ-test/scripts"

submit_step() {
    local name="$1"
    local script="$2"

    echo "Submitting: ${name}..."
    STEP_ID=$(aws emr add-steps \
        --cluster-id "${CLUSTER_ID}" \
        --region "${REGION}" \
        --steps "[{
            \"Type\": \"CUSTOM_JAR\",
            \"Name\": \"${name}\",
            \"ActionOnFailure\": \"CONTINUE\",
            \"Jar\": \"s3://${REGION}.elasticmapreduce/libs/script-runner/script-runner.jar\",
            \"Args\": [
                \"${S3_SCRIPTS}/run-spark-sql.sh\",
                \"${S3_SCRIPTS}/${script}\"
            ]
        }]" \
        --query 'StepIds[0]' --output text)

    echo "  Step ID: ${STEP_ID} — waiting..."
    # Don't let set -e kill us on waiter failure
    aws emr wait step-complete \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --region "${REGION}" 2>&1 || true

    STATUS=$(aws emr describe-step \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --region "${REGION}" \
        --query 'Step.Status.State' --output text)

    if [ "${STATUS}" != "COMPLETED" ]; then
        echo "  ✗ Step '${name}' failed: ${STATUS}"
        REASON=$(aws emr describe-step \
            --cluster-id "${CLUSTER_ID}" \
            --step-id "${STEP_ID}" \
            --region "${REGION}" \
            --query 'Step.Status.FailureDetails' --output json 2>/dev/null || echo "unknown")
        echo "  Failure details: ${REASON}"

        # Try to fetch logs from S3
        STEP_LOG_BASE="s3://${S3_BUCKET}/integ-test/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}"
        for LOG_TYPE in stderr stdout syslog; do
            LOG_FILE="${STEP_LOG_BASE}/${LOG_TYPE}.gz"
            echo "  Checking step ${LOG_TYPE} at: ${LOG_FILE}"
            if aws s3 cp "${LOG_FILE}" "/tmp/step-${LOG_TYPE}.gz" --region "${REGION}" 2>/dev/null; then
                gunzip -f "/tmp/step-${LOG_TYPE}.gz" 2>/dev/null && \
                    echo "  --- Step ${LOG_TYPE} (last 80 lines) ---" && \
                    tail -80 "/tmp/step-${LOG_TYPE}"
            else
                echo "  (${LOG_TYPE} log not available yet)"
            fi
        done
        return 1
    fi
    echo "  ✓ ${name} completed"
}

submit_step "Create Hive Tables" "01_create_hive_tables.sql"
submit_step "Create Iceberg Tables" "02_create_iceberg_tables.sql"
submit_step "Alter and Drop" "03_alter_and_drop.sql"

# Wait for sync agent batch window (configured to 10s in CFN, wait 30s for safety)
echo ""
echo "Waiting 30s for sync agent to process events..."
sleep 30

submit_step "Drop Table" "04_drop_table.sql"

echo "Waiting 30s for drop to sync..."
sleep 30

# ─── 4. Validate ───────────────────────────────────────────────────────────
echo ""
echo "=== Running Validation ==="

python3 "${SCRIPT_DIR}/validate/validate_sync.py" \
    --database "${DATABASE}" \
    --s3-output "s3://${S3_BUCKET}/integ-test/athena-results/" \
    --region "${REGION}"

VALIDATION_EXIT=$?

echo ""
if [ ${VALIDATION_EXIT} -eq 0 ]; then
    echo "=== Integration Tests PASSED ==="
else
    echo "=== Integration Tests FAILED ==="
fi

exit ${VALIDATION_EXIT}
