#!/usr/bin/env bash
# One affected-crate compile, followed by the complete host suite and real Dex.
set -euo pipefail
trap 'rc=$?; echo "STEP_FAILED oidc-verify line=$LINENO rc=$rc; recovery: bash scripts/qualify-oidc.sh" >&2' ERR
node scripts/qualify-rustfmt.mjs
cargo test -p acyclic-agent-host-conformance --features local --no-run
cargo test -p acyclic-agent-host-conformance --features local
bash scripts/capture-oidc-local.sh
