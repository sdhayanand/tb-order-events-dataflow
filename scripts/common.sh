#!/usr/bin/env bash
# Shared settings for the gcloud scripts. Override anything with environment variables.
set -euo pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "${PROJECT}" ]]; then
  echo "PROJECT is not set and gcloud has no default project" >&2
  exit 1
fi
REGION="${REGION:-us-central1}"
AR_REPO="${AR_REPO:-tb-otd}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-gs://${PROJECT}-tb-otd-dataflow}"
TEMPLATE_DIR="${TEMPLATE_BUCKET}/templates"
STAGING_LOCATION="${STAGING_LOCATION:-${TEMPLATE_BUCKET}/staging}"
TEMP_LOCATION="${TEMP_LOCATION:-${TEMPLATE_BUCKET}/temp}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-dataflow-runner@${PROJECT}.iam.gserviceaccount.com}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
BASE_IMAGE="${BASE_IMAGE:-gcr.io/dataflow-templates-base/java21-template-launcher-base:latest}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(cd "${REPO_ROOT}" && mvn -q -ntp help:evaluate -Dexpression=project.version -DforceStdout 2>/dev/null || echo 1.0.0)"
BUNDLED_JAR="${BUNDLED_JAR:-${REPO_ROOT}/target/tb-order-events-dataflow-${VERSION}-bundled.jar}"

STREAMING_MAIN="com.tailoredbrands.otd.dataflow.OrderEventsStreamingPipeline"
BATCH_MAIN="com.tailoredbrands.otd.dataflow.DailyReconciliationPipeline"

image_for() { echo "${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/dataflow-$1:${IMAGE_TAG}"; }
template_for() { echo "${TEMPLATE_DIR}/$1.json"; }
