#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/check-host-serving.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
step conformance-wasm cargo check -p acyclic-cloudflare-conformance --target wasm32-unknown-unknown --locked
step native-host cargo test -p acyclic-agent-host-conformance --features local --no-run --locked
step portable-wasm-host cargo check -p acyclic-harness --target wasm32-unknown-unknown --no-default-features --features hosting --locked
