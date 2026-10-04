#!/usr/bin/env bash
# Launches the order-events-streaming Flex Template on Dataflow (Streaming Engine, autoscaling 1-2
# n1-standard-2 workers, dedicated runner service account).
#
#   PROJECT=my-project ./scripts/run-streaming.sh
#   STORE_REF=gs://my-project-tb-otd-dataflow/reference/stores.csv PROJECT=my-project ./scripts/run-streaming.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

JOB_NAME="${JOB_NAME:-order-events-streaming-$(date -u +%Y%m%d-%H%M%S)}"
DATASET="${DATASET:-otd}"
MAX_WORKERS="${MAX_WORKERS:-2}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-2}"
STORE_REF="${STORE_REF:-}"
AT_LEAST_ONCE="${AT_LEAST_ONCE:-false}"

PARAMS="ordersSubscription=projects/${PROJECT}/subscriptions/orders-dataflow"
PARAMS+=",inventorySubscription=projects/${PROJECT}/subscriptions/inventory-dataflow"
PARAMS+=",shipmentsSubscription=projects/${PROJECT}/subscriptions/shipments-dataflow"
PARAMS+=",deadLetterTopic=projects/${PROJECT}/topics/events-dlq"
PARAMS+=",bigQueryDataset=${DATASET}"
PARAMS+=",metricsWindowMinutes=${METRICS_WINDOW_MINUTES:-1}"
PARAMS+=",useStorageApiAtLeastOnce=${AT_LEAST_ONCE}"
if [[ -n "${STORE_REF}" ]]; then
  PARAMS+=",storeReferenceGcsPath=${STORE_REF}"
fi

echo ">> Launching ${JOB_NAME} in ${PROJECT}/${REGION}"
gcloud dataflow flex-template run "${JOB_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --template-file-gcs-location "$(template_for order-events-streaming)" \
  --service-account-email "${SERVICE_ACCOUNT}" \
  --staging-location "${STAGING_LOCATION}" \
  --temp-location "${TEMP_LOCATION}" \
  --enable-streaming-engine \
  --num-workers 1 \
  --max-workers "${MAX_WORKERS}" \
  --worker-machine-type "${MACHINE_TYPE}" \
  --parameters "${PARAMS}"

echo ">> Job launched. Watch it with:"
echo "   gcloud dataflow jobs list --project ${PROJECT} --region ${REGION} --filter 'name=${JOB_NAME}'"
