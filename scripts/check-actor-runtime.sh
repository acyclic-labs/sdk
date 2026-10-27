#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/check-actor-runtime.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
step actor-tests cargo test -p acyclic-actors --no-run --locked
step agent-tests cargo test -p acyclic-harness-core --features filesystem --test agent-model --no-run --locked
step host-tests cargo test -p acyclic-agent-host-conformance --features local --no-run --locked
step portable-host cargo check -p acyclic-cloudflare-conformance --target wasm32-unknown-unknown --locked
