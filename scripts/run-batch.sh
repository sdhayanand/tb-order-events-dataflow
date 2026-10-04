#!/usr/bin/env bash
# Launches the daily-reconciliation Flex Template for one business date (default: yesterday, UTC).
# Cloud Scheduler / Composer call the same gcloud command (see tb-orchestration).
#
#   PROJECT=my-project ./scripts/run-batch.sh 2026-10-03
#   PROJECT=my-project EXTRACT_BUCKET=gs://my-bucket ./scripts/run-batch.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

RUN_DATE="${1:-$(date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)}"
DATASET="${DATASET:-otd}"
EXTRACT_BUCKET="${EXTRACT_BUCKET:-${TEMPLATE_BUCKET}}"
LEGACY_EXTRACT_PATH="${LEGACY_EXTRACT_PATH:-${EXTRACT_BUCKET}/oms-extract/${RUN_DATE}/*.xml}"
REPORT_PATH="${REPORT_PATH:-${EXTRACT_BUCKET}/reports/reconciliation-${RUN_DATE}.csv}"
JOB_NAME="${JOB_NAME:-daily-reconciliation-${RUN_DATE//-/}-$(date -u +%H%M%S)}"
MAX_WORKERS="${MAX_WORKERS:-2}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-2}"

PARAMS="runDate=${RUN_DATE}"
PARAMS+=",legacyExtractPath=${LEGACY_EXTRACT_PATH}"
PARAMS+=",reportGcsPath=${REPORT_PATH}"
PARAMS+=",bigQueryDataset=${DATASET}"

echo ">> Launching ${JOB_NAME} for ${RUN_DATE} in ${PROJECT}/${REGION}"
gcloud dataflow flex-template run "${JOB_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --template-file-gcs-location "$(template_for daily-reconciliation)" \
  --service-account-email "${SERVICE_ACCOUNT}" \
  --staging-location "${STAGING_LOCATION}" \
  --temp-location "${TEMP_LOCATION}" \
  --num-workers 1 \
  --max-workers "${MAX_WORKERS}" \
  --worker-machine-type "${MACHINE_TYPE}" \
  --parameters "${PARAMS}"

echo ">> When the job finishes:"
echo "   gsutil cat ${REPORT_PATH}"
echo "   bq query --use_legacy_sql=false 'SELECT classification, COUNT(*) c FROM \`${PROJECT}.${DATASET}.order_reconciliation\` WHERE run_date = \"${RUN_DATE}\" GROUP BY 1'"
