#!/usr/bin/env bash
# Builds the bundled jar and both Flex Templates (launcher images in Artifact Registry + template
# specs in GCS) with `gcloud dataflow flex-template build`, which runs Cloud Build in the project.
#
#   PROJECT=my-project ./scripts/build-template.sh            # both templates
#   PROJECT=my-project ./scripts/build-template.sh streaming  # only order-events-streaming
#   PROJECT=my-project ./scripts/build-template.sh batch      # only daily-reconciliation
#
# Prerequisites (created by tb-platform-infra Terraform): bucket gs://$PROJECT-tb-otd-dataflow,
# Artifact Registry repo tb-otd in $REGION, Cloud Build API enabled.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

WHICH="${1:-all}"

if [[ ! -f "${BUNDLED_JAR}" || "${SKIP_BUILD:-false}" != "true" ]]; then
  echo ">> Building bundled jar"
  (cd "${REPO_ROOT}" && mvn -B -ntp -q -Pdataflow -DskipTests package)
fi
[[ -f "${BUNDLED_JAR}" ]] || { echo "bundled jar not found: ${BUNDLED_JAR}" >&2; exit 1; }

build_one() {
  local name="$1" main_class="$2" metadata="$3"
  echo ">> Building Flex Template ${name}"
  gcloud dataflow flex-template build "$(template_for "${name}")" \
    --project "${PROJECT}" \
    --image-gcr-path "$(image_for "${name}")" \
    --sdk-language JAVA \
    --flex-template-base-image "${BASE_IMAGE}" \
    --metadata-file "${REPO_ROOT}/metadata/${metadata}" \
    --jar "${BUNDLED_JAR}" \
    --env "FLEX_TEMPLATE_JAVA_MAIN_CLASS=${main_class}"
  echo "   template spec: $(template_for "${name}")"
  echo "   launcher image: $(image_for "${name}")"
}

case "${WHICH}" in
  streaming) build_one order-events-streaming "${STREAMING_MAIN}" order-events-streaming-metadata.json ;;
  batch)     build_one daily-reconciliation "${BATCH_MAIN}" daily-reconciliation-metadata.json ;;
  all)
    build_one order-events-streaming "${STREAMING_MAIN}" order-events-streaming-metadata.json
    build_one daily-reconciliation "${BATCH_MAIN}" daily-reconciliation-metadata.json
    ;;
  *) echo "usage: $0 [streaming|batch|all]" >&2; exit 2 ;;
esac
