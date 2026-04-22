#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEG_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Tee all output to a log file while keeping it on stdout/stderr
LOAD_LOG="${INTEG_DIR}/../target/load-test-output.log"
mkdir -p "$(dirname "${LOAD_LOG}")" 2>/dev/null || true
exec > >(tee -a "${LOAD_LOG}") 2>&1

# ─── Load test orchestration ───────────────────────────────────────────────
# Generates DDL, submits to EMR, waits for sync, validates results.
#
# Environment variables (required):
#   TEST_S3_BUCKET         - S3 bucket for test artifacts
#   SUBNET_ID              - VPC subnet for EMR cluster
#
# Optional:
#   AWS_REGION             - defaults to us-east-1
#   SCENARIO               - comma-separated scenarios (default: burst-create)
#   BATCH_WINDOW_SECONDS   - sync agent batch window (default: 10)
#   EMR_RELEASE            - defaults to emr-7.1.0
#   GLUE_CATALOG_ID        - defaults to empty (account default)
# ────────────────────────────────────────────────────────────────────────────

S3_BUCKET="${TEST_S3_BUCKET:?Set TEST_S3_BUCKET}"
SUBNET="${SUBNET_ID:?Set SUBNET_ID}"
REGION="${AWS_REGION:-us-east-1}"
SCENARIOS="${SCENARIO:-burst-create}"
BATCH_WINDOW="${BATCH_WINDOW_SECONDS:-10}"
EMR_RELEASE="${EMR_RELEASE:-emr-7.1.0}"
CATALOG_ID="${GLUE_CATALOG_ID:-}"
STACK_NAME="hms-gdc-load-test-$(date +%s)"
S3_JAR_KEY="load-test/jars/HiveGlueCatalogSyncAgent-complete.jar"
S3_BOOTSTRAP_KEY="load-test/scripts/bootstrap-install-agent.sh"
METRICS_NAMESPACE="HiveGlueCatalogSync"
CLUSTER_ID=""

# Track all databases created across scenarios for cleanup
CREATED_DATABASES=()

# ─── Scenario expected counts ──────────────────────────────────────────────
# Returns: expected_tables expected_partitions databases_list
get_scenario_counts() {
    local scenario="$1"
    case "${scenario}" in
        burst-create)
            echo "100 0 load_test_db_0"
            ;;
        partition-heavy)
            echo "5 1000 load_test_db_0"
            ;;
        multi-db)
            local dbs=""
            for i in $(seq 0 9); do
                dbs="${dbs:+${dbs} }load_test_db_${i}"
            done
            echo "100 1000 ${dbs}"
            ;;
        mixed-ops)
            # 50 tables created, 10 dropped (every 5th), 40 remain
            echo "40 800 load_test_db_0"
            ;;
        sustained)
            echo "500 2500 load_test_db_0"
            ;;
        *)
            echo "ERROR: Unknown scenario '${scenario}'" >&2
            return 1
            ;;
    esac
}

# Find the sync agent fat JAR — fail early with a clear message
SYNC_AGENT_JAR=""
for f in "${INTEG_DIR}/../target/HiveGlueCatalogSyncAgent-"*"-complete.jar"; do
    if [ -f "$f" ]; then
        SYNC_AGENT_JAR="$f"
        break
    fi
done
if [ -z "${SYNC_AGENT_JAR}" ]; then
    echo "ERROR: Sync agent fat JAR not found in target/"
    echo "Run 'mvn package assembly:assembly -DskipTests' first."
    exit 1
fi

# ─── Cleanup ────────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "=== Cleanup ==="

    # Delete all Glue databases/tables created during load tests
    for db in "${CREATED_DATABASES[@]+"${CREATED_DATABASES[@]}"}"; do
        echo "Cleaning up Glue database: ${db}..."
        TABLES=$(aws glue get-tables --database-name "${db}" --region "${REGION}" \
            --query 'TableList[].Name' --output text 2>/dev/null || true)
        for t in ${TABLES}; do
            aws glue delete-table --database-name "${db}" --name "${t}" --region "${REGION}" 2>/dev/null || true
        done
        aws glue delete-database --name "${db}" --region "${REGION}" 2>/dev/null || true
    done

    # Delete CFN stack (terminates EMR cluster)
    if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" &>/dev/null; then
        echo "Deleting CloudFormation stack ${STACK_NAME}..."
        aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
        aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" --region "${REGION}" || true
    fi

    # Clean up S3 test data
    echo "Cleaning up S3 test data..."
    aws s3 rm "s3://${S3_BUCKET}/load-test/" --recursive --region "${REGION}" 2>/dev/null || true

    echo "Cleanup complete."
}

trap cleanup EXIT

echo "============================================================"
echo "  HMS-to-GDC Sync Agent Load Test"
echo "============================================================"
echo "Stack:        ${STACK_NAME}"
echo "Bucket:       ${S3_BUCKET}"
echo "Subnet:       ${SUBNET}"
echo "Region:       ${REGION}"
echo "Scenarios:    ${SCENARIOS}"
echo "Batch Window: ${BATCH_WINDOW}s"
echo "Release:      ${EMR_RELEASE}"
echo ""

# ─── 1. Upload artifacts to S3 ─────────────────────────────────────────────
echo "=== Uploading artifacts to S3 ==="

# Sync agent JAR
echo "Uploading sync agent JAR: ${SYNC_AGENT_JAR}"
aws s3 cp "${SYNC_AGENT_JAR}" "s3://${S3_BUCKET}/${S3_JAR_KEY}" --region "${REGION}"

# Bootstrap script
echo "Uploading bootstrap script..."
aws s3 cp "${INTEG_DIR}/scripts/bootstrap-install-agent.sh" \
    "s3://${S3_BUCKET}/${S3_BOOTSTRAP_KEY}" --region "${REGION}"

# Spark SQL wrapper script
echo "Uploading spark-sql wrapper script..."
aws s3 cp "${INTEG_DIR}/scripts/run-spark-sql.sh" \
    "s3://${S3_BUCKET}/load-test/scripts/run-spark-sql.sh" --region "${REGION}"

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

# Deploy using the existing CFN template with modifications:
# - Override batch window seconds to the configured value
# - Inject metrics configuration (enabled + namespace)
MODIFIED_TEMPLATE=$(mktemp /tmp/emr-cfn.XXXXXXXXXX)
mv "${MODIFIED_TEMPLATE}" "${MODIFIED_TEMPLATE}.yaml"
MODIFIED_TEMPLATE="${MODIFIED_TEMPLATE}.yaml"
sed \
    -e "s|glue.catalog.batch.window.seconds: '10'|glue.catalog.batch.window.seconds: '${BATCH_WINDOW}'|g" \
    "${INTEG_DIR}/cfn/emr-integ-test.yaml" > "${MODIFIED_TEMPLATE}"

# Inject metrics configuration lines after the batch.window.seconds line
sed -i '' "/glue.catalog.batch.window.seconds:/a\\
            glue.catalog.metrics.enabled: 'true'\\
            glue.catalog.metrics.namespace: '${METRICS_NAMESPACE}'" \
    "${MODIFIED_TEMPLATE}"

aws cloudformation create-stack \
    --stack-name "${STACK_NAME}" \
    --template-body "file://${MODIFIED_TEMPLATE}" \
    --parameters ${PARAMS} \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}"

rm -f "${MODIFIED_TEMPLATE}"

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

# ─── 3. EMR step helper ────────────────────────────────────────────────────
S3_SCRIPTS="s3://${S3_BUCKET}/load-test/scripts"

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
        STEP_LOG_BASE="s3://${S3_BUCKET}/load-test/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}"
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

# ─── 4. Run scenarios sequentially ─────────────────────────────────────────
OVERALL_EXIT=0
IFS=',' read -ra SCENARIO_LIST <<< "${SCENARIOS}"

for CURRENT_SCENARIO in "${SCENARIO_LIST[@]}"; do
    CURRENT_SCENARIO=$(echo "${CURRENT_SCENARIO}" | xargs)  # trim whitespace
    echo ""
    echo "============================================================"
    echo "  Running scenario: ${CURRENT_SCENARIO}"
    echo "============================================================"

    # Get expected counts for this scenario
    COUNTS=$(get_scenario_counts "${CURRENT_SCENARIO}")
    EXPECTED_TABLES=$(echo "${COUNTS}" | awk '{print $1}')
    EXPECTED_PARTITIONS=$(echo "${COUNTS}" | awk '{print $2}')
    DATABASES_LIST=$(echo "${COUNTS}" | awk '{for(i=3;i<=NF;i++) print $i}')

    # Track databases for cleanup
    for db in ${DATABASES_LIST}; do
        CREATED_DATABASES+=("${db}")
    done

    # Use the first database for validation
    PRIMARY_DB=$(echo "${DATABASES_LIST}" | head -1)

    echo "Expected tables:     ${EXPECTED_TABLES}"
    echo "Expected partitions: ${EXPECTED_PARTITIONS}"
    echo "Primary database:    ${PRIMARY_DB}"

    # ─── 4a. Generate DDL ───────────────────────────────────────────────
    echo ""
    echo "--- Generating DDL ---"
    DDL_DIR=$(mktemp -d /tmp/load-test-ddl-XXXXXX)

    python3 "${SCRIPT_DIR}/generate-load.py" \
        --scenario "${CURRENT_SCENARIO}" \
        --output-dir "${DDL_DIR}" \
        --s3-bucket "${S3_BUCKET}"

    # ─── 4b. Upload DDL to S3 ──────────────────────────────────────────
    echo ""
    echo "--- Uploading DDL to S3 ---"
    for sql_file in "${DDL_DIR}"/*.sql; do
        BASENAME=$(basename "${sql_file}")
        aws s3 cp "${sql_file}" \
            "s3://${S3_BUCKET}/load-test/scripts/${BASENAME}" \
            --region "${REGION}"
    done
    rm -rf "${DDL_DIR}"

    # ─── 4c. Submit EMR steps ──────────────────────────────────────────
    echo ""
    echo "--- Submitting EMR steps ---"
    STEP_FAILED=0
    for sql_file in $(aws s3 ls "s3://${S3_BUCKET}/load-test/scripts/${CURRENT_SCENARIO}_" --region "${REGION}" | awk '{print $4}'); do
        if ! submit_step "Load Test: ${CURRENT_SCENARIO} - ${sql_file}" "${sql_file}"; then
            STEP_FAILED=1
            echo "WARNING: Step failed for ${sql_file}, continuing to validation..."
        fi
    done

    # ─── 4d. Wait for sync agent to process events ─────────────────────
    # Wait proportional to batch window + buffer for processing
    SYNC_WAIT=$(( BATCH_WINDOW * 3 + 30 ))
    echo ""
    echo "Waiting ${SYNC_WAIT}s for sync agent to process events..."
    sleep "${SYNC_WAIT}"

    # ─── 4e. Run validation ────────────────────────────────────────────
    echo ""
    echo "--- Running validation for ${CURRENT_SCENARIO} ---"
    VALIDATION_EXIT=0
    python3 "${SCRIPT_DIR}/validate-load.py" \
        --database "${PRIMARY_DB}" \
        --region "${REGION}" \
        --scenario "${CURRENT_SCENARIO}" \
        --expected-tables "${EXPECTED_TABLES}" \
        --expected-partitions "${EXPECTED_PARTITIONS}" \
        --metrics-namespace "${METRICS_NAMESPACE}" \
        --s3-output "s3://${S3_BUCKET}/load-test/reports/${CURRENT_SCENARIO}-report.json" \
        || VALIDATION_EXIT=$?

    if [ ${VALIDATION_EXIT} -ne 0 ]; then
        echo "✗ Scenario '${CURRENT_SCENARIO}' FAILED"
        OVERALL_EXIT=1
    else
        echo "✓ Scenario '${CURRENT_SCENARIO}' PASSED"
    fi

    # Clean up scenario-specific DDL files from S3
    aws s3 rm "s3://${S3_BUCKET}/load-test/scripts/${CURRENT_SCENARIO}_" \
        --recursive --region "${REGION}" 2>/dev/null || true

done

# ─── 5. Final summary ──────────────────────────────────────────────────────
echo ""
echo "============================================================"
if [ ${OVERALL_EXIT} -eq 0 ]; then
    echo "  Load Tests PASSED (all scenarios)"
else
    echo "  Load Tests FAILED (one or more scenarios)"
fi
echo "============================================================"

exit ${OVERALL_EXIT}
