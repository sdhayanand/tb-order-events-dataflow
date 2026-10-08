#!/usr/bin/env bash
# Production-style launch of the hello job: from its Flex Template, no Java or Maven needed here.
# This is what Cloud Scheduler, Composer, the Console ("Create job from template") or CI do.
#   PROJECT=crosscutdata-509514 ./scripts/run-hello-template.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
JOB_NAME="${JOB_NAME:-store-revenue-$(date -u +%Y%m%d-%H%M%S)}"
IN="${IN:-${TEMPLATE_BUCKET}/hello/input/orders-sample.csv}"
OUT="${TEMPLATE_BUCKET}/hello/output/${JOB_NAME}/store-revenue"
gcloud storage cp -q "${REPO_ROOT}/examples/orders-sample.csv" "$IN" 2>/dev/null || gsutil -q cp "${REPO_ROOT}/examples/orders-sample.csv" "$IN"
echo ">> launching ${JOB_NAME} from $(template_for store-revenue)"
gcloud dataflow flex-template run "$JOB_NAME" \
  --project "$PROJECT" --region "$REGION" \
  --template-file-gcs-location "$(template_for store-revenue)" \
  --service-account-email "$SERVICE_ACCOUNT" \
  --staging-location "$STAGING_LOCATION" --temp-location "$TEMP_LOCATION" \
  --launcher-machine-type n1-standard-1 --worker-machine-type n1-standard-1 \
  --num-workers 1 --max-workers 1 \
  --parameters "input=${IN},output=${OUT}"
bash "${REPO_ROOT}/scripts/wait-job.sh" "$JOB_NAME"
echo ">> result:"; gsutil cat "${OUT}.csv"
