#!/usr/bin/env bash
set -euo pipefail
trap 'rc=$?; echo "STEP_FAILED be-edge-verify line=$LINENO rc=$rc; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-verify.sh" >&2' ERR
cargo fmt --all -- --check
cargo test -p acyclic-conformance -p acyclic-harness-providers \
    --features acyclic-conformance/inference --no-run
provider_test_status=0
if cargo test -p acyclic-conformance -p acyclic-harness-providers \
    --features acyclic-conformance/inference --no-fail-fast; then
    :
else
    provider_test_status=$?
fi
BE_EDGE_RUN_SUITE=1 bash scripts/be-edge-local.sh
if [ "$provider_test_status" != 0 ]; then
    echo "STEP_FAILED provider-tests rc=$provider_test_status; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 cargo test -p acyclic-conformance -p acyclic-harness-providers --features acyclic-conformance/inference --no-fail-fast" >&2
    exit "$provider_test_status"
fi
