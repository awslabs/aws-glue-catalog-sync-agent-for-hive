#!/bin/bash
set -euo pipefail

# Bootstrap action: downloads the sync agent JAR into Hive's auxiliary lib directory
# so it's loaded as a MetaStoreEventListener.
#
# Usage: bootstrap-install-agent.sh <s3-jar-path>

JAR_S3_PATH="${1:?Usage: bootstrap-install-agent.sh <s3-jar-path>}"
HIVE_AUX_DIR="/usr/lib/hive/auxlib"

echo "Installing sync agent JAR from ${JAR_S3_PATH}..."

sudo mkdir -p "${HIVE_AUX_DIR}"
sudo aws s3 cp "${JAR_S3_PATH}" "${HIVE_AUX_DIR}/HiveGlueCatalogSyncAgent.jar"
sudo chmod 644 "${HIVE_AUX_DIR}/HiveGlueCatalogSyncAgent.jar"

echo "Sync agent JAR installed to ${HIVE_AUX_DIR}/HiveGlueCatalogSyncAgent.jar"
