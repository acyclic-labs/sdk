#!/usr/bin/env bash
# One affected-crate compile, followed by the complete host suite and real Dex.
set -euo pipefail
trap 'rc=$?; echo "STEP_FAILED be-edge-oidc-verify line=$LINENO rc=$rc; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-oidc-verify.sh" >&2' ERR
cargo fmt --all -- --check
cargo test -p acyclic-agent-host-conformance --features local --no-run
cargo test -p acyclic-agent-host-conformance --features local
bash scripts/be-edge-oidc.sh
