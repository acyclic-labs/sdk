#!/usr/bin/env bash
set -euo pipefail

recovery='CLOUD_SOURCE=/absolute/cloud CLOUD_BINARIES=/absolute/binaries SDK_ARTIFACT_DIR=/absolute/results cargo nextest run --profile full-cloud --locked --all-features -p acyclic-fs -p acyclic-harness'
for name in CLOUD_SOURCE CLOUD_BINARIES SDK_ARTIFACT_DIR; do
  if [[ "${!name:-}" != /* ]]; then
    echo "STEP_FAILED admit-full-cloud: $name must name an absolute path; recovery: $recovery" >&2
    exit 2
  fi
done
result=$(mktemp -d "$SDK_ARTIFACT_DIR/full-cloud.XXXXXX") || {
  echo "STEP_FAILED create-full-cloud-result; recovery: $recovery" >&2
  exit 2
}
# Nextest supplies this exact test binary and its arguments. The shared producer
# owns readiness, endpoint injection and teardown for this one fresh topology.
node "$(dirname -- "${BASH_SOURCE[0]}")/local-cloud.mjs" \
  --cloud "$CLOUD_SOURCE" --binaries "$CLOUD_BINARIES" \
  --out "$result/topology" --topology full -- "$@" || {
  status=$?
  echo "STEP_FAILED qualify-full-cloud-case: status=$status; recovery: $recovery" >&2
  exit "$status"
}
