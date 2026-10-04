#!/usr/bin/env bash
# Runs OrderEventsStreamingPipeline with the DirectRunner against the Pub/Sub emulator and writes
# JSON lines files (one per BigQuery table) to --localOutputDir instead of BigQuery.  No GCP
# credentials, no BigQuery, no Dataflow: this is what the platform e2e in tb-platform-infra/local
# uses to verify the pipeline end to end.
#
#   PUBSUB_EMULATOR_HOST=localhost:8085 ./scripts/local-direct-runner.sh ./out
#
# The emulator must already have the topics/subscriptions (tb-platform-infra/local/pubsub-init.sh):
#   orders-v1 / orders-dataflow, inventory-v1 / inventory-dataflow, shipments-v1 / shipments-dataflow
#
# Env:  PUBSUB_EMULATOR_HOST (default localhost:8085)   PUBSUB_PROJECT (default local-project)
#       ORDERS_SUB / INVENTORY_SUB / SHIPMENTS_SUB       METRICS_WINDOW_MINUTES (default 1)
#       STORE_REF (optional CSV path)                    SKIP_BUILD=true to reuse the bundled jar
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-${REPO_ROOT}/out}"
export PUBSUB_EMULATOR_HOST="${PUBSUB_EMULATOR_HOST:-localhost:8085}"
PUBSUB_PROJECT="${PUBSUB_PROJECT:-local-project}"
ORDERS_SUB="${ORDERS_SUB:-projects/${PUBSUB_PROJECT}/subscriptions/orders-dataflow}"
INVENTORY_SUB="${INVENTORY_SUB:-projects/${PUBSUB_PROJECT}/subscriptions/inventory-dataflow}"
SHIPMENTS_SUB="${SHIPMENTS_SUB:-projects/${PUBSUB_PROJECT}/subscriptions/shipments-dataflow}"
METRICS_WINDOW_MINUTES="${METRICS_WINDOW_MINUTES:-1}"
STORE_REF="${STORE_REF:-}"

VERSION="$(cd "${REPO_ROOT}" && mvn -q -ntp help:evaluate -Dexpression=project.version -DforceStdout 2>/dev/null || echo 1.0.0)"
BUNDLED_JAR="${BUNDLED_JAR:-${REPO_ROOT}/target/tb-order-events-dataflow-${VERSION}-bundled.jar}"

if [[ "${SKIP_BUILD:-false}" != "true" || ! -f "${BUNDLED_JAR}" ]]; then
  echo ">> Building bundled jar (DirectRunner included)"
  (cd "${REPO_ROOT}" && mvn -B -ntp -q -Pdataflow -DskipTests package)
fi

mkdir -p "${OUT_DIR}"
ARGS=(
  "--runner=DirectRunner"
  "--streaming=true"
  "--project=${PUBSUB_PROJECT}"
  "--pubsubRootUrl=http://${PUBSUB_EMULATOR_HOST}"
  "--ordersSubscription=${ORDERS_SUB}"
  "--inventorySubscription=${INVENTORY_SUB}"
  "--shipmentsSubscription=${SHIPMENTS_SUB}"
  "--metricsWindowMinutes=${METRICS_WINDOW_MINUTES}"
  "--localOutputDir=${OUT_DIR}"
)
if [[ -n "${STORE_REF}" ]]; then
  ARGS+=("--storeReferenceGcsPath=${STORE_REF}")
fi

echo ">> DirectRunner against emulator ${PUBSUB_EMULATOR_HOST}, output ${OUT_DIR}"
echo "   (Ctrl-C / SIGTERM to stop; files are appended as events arrive)"
exec java -cp "${BUNDLED_JAR}" \
  com.tailoredbrands.otd.dataflow.OrderEventsStreamingPipeline "${ARGS[@]}"
