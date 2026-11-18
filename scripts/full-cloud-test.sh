#!/usr/bin/env bash
set -euo pipefail

recovery='CLOUD_SOURCE=/absolute/cloud CLOUD_BINARIES=/absolute/binaries SDK_ARTIFACT_DIR=/absolute/results bazel test --test_env=CLOUD_SOURCE --test_env=CLOUD_BINARIES --test_env=SDK_ARTIFACT_DIR //rust/crates/filesystem:acyclic_fs_test_full-cloud //rust/crates/filesystem:main_full-cloud //rust/crates/harness:acyclic_harness_test_full-cloud //rust/crates/harness:main_full-cloud'
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
# The Bazel test runner supplies the exact test binary and arguments. The shared producer
# owns readiness, endpoint injection and teardown for this one fresh topology.
"${CARGO_BIN_EXE_node:-node}" "$(dirname -- "${BASH_SOURCE[0]}")/local-cloud.mjs" \
  --cloud "$CLOUD_SOURCE" --binaries "$CLOUD_BINARIES" \
  --out "$result/topology" --topology full -- "$@" || {
  status=$?
  echo "STEP_FAILED qualify-full-cloud-case: status=$status; recovery: $recovery" >&2
  exit "$status"
}
