#!/usr/bin/env bash
# Wait for a Dataflow job (by name) to finish; prints its state transitions. Exit 0 only on Done.
#   PROJECT=... REGION=us-central1 ./scripts/wait-job.sh <job-name> [timeout-minutes]
set -euo pipefail
NAME="${1:?job name}"; LIMIT="${2:-25}"
PROJECT="${PROJECT:?set PROJECT}"; REGION="${REGION:-us-central1}"
last=""; end=$(( $(date +%s) + LIMIT * 60 ))
while [ "$(date +%s)" -lt "$end" ]; do
  state=$(gcloud dataflow jobs list --project "$PROJECT" --region "$REGION" --filter="name=${NAME}" \
            --format='value(state)' --limit=1 2>/dev/null || true)
  [ "$state" != "$last" ] && { echo "   $(date -u +%H:%M:%S) ${NAME}: ${state:-not visible yet}"; last="$state"; }
  case "$state" in
    Done) exit 0 ;;
    Failed|Cancelled) exit 1 ;;
  esac
  sleep 15
done
echo "   timed out after ${LIMIT} min"; exit 1
