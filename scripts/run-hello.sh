#!/usr/bin/env bash
# The simplest Dataflow job in this repo (StoreRevenueJob): CSV of orders -> revenue per store.
#   PROJECT=crosscutdata-509514 ./scripts/run-hello.sh            # on Dataflow (watch it in the console)
#   RUNNER=DirectRunner ./scripts/run-hello.sh                    # on this machine, no GCP needed
set -euo pipefail
RUNNER="${RUNNER:-DataflowRunner}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ "$RUNNER" == DirectRunner ]]; then
  mvn -q -ntp compile exec:java -Dexec.mainClass=com.tailoredbrands.otd.dataflow.examples.StoreRevenueJob \
    -Dexec.args="--runner=DirectRunner --input=examples/orders-sample.csv --output=target/hello/store-revenue"
  cat target/hello/store-revenue.csv
  exit 0
fi
source scripts/common.sh
JOB_NAME="${JOB_NAME:-hello-store-revenue-$(date -u +%Y%m%d-%H%M%S)}"
IN="${TEMPLATE_BUCKET}/hello/input/orders-sample.csv"
OUT="${TEMPLATE_BUCKET}/hello/output/${JOB_NAME}/store-revenue"
gsutil -q cp examples/orders-sample.csv "$IN"
echo ">> ${JOB_NAME}: https://console.cloud.google.com/dataflow/jobs?project=${PROJECT}"
mvn -q -ntp compile exec:java -Dexec.mainClass=com.tailoredbrands.otd.dataflow.examples.StoreRevenueJob \
  -Dexec.args="--runner=DataflowRunner --project=${PROJECT} --region=${REGION} --jobName=${JOB_NAME} \
--serviceAccount=${SERVICE_ACCOUNT} --tempLocation=${TEMP_LOCATION} --stagingLocation=${STAGING_LOCATION} \
--numWorkers=1 --maxNumWorkers=1 --workerMachineType=n1-standard-1 --input=${IN} --output=${OUT}"
echo ">> result (${OUT}.csv):"
gsutil cat "${OUT}.csv"
