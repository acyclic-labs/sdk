#!/usr/bin/env bash
set -u
step() {
  local name=$1
  shift
  if "$@"; then return 0; else
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash -c "bash scripts/check-actor-delivery.sh"\n' "$name" "$status" >&2
    exit "$status"
  fi
}
step format cargo fmt --all -- --check
step native cargo check -p acyclic-actors -p acyclic-harness-core -p acyclic-agent-host-conformance --tests --features acyclic-harness-core/filesystem,acyclic-harness-core/test-support,acyclic-agent-host-conformance/local --locked
step portable cargo check -p acyclic-cloudflare-conformance --target wasm32-unknown-unknown --locked
