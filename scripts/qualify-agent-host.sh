#!/usr/bin/env bash
set -euo pipefail

step() {
  local name=$1
  shift
  if "$@"; then return 0; else
    local rc=$?
    printf 'STEP %s failed (%s); recovery:' "$name" "$rc" >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
    return "$rc"
  fi
}

step native-host cargo test -p acyclic-agent-host-conformance --features local
step browser-host-compile cargo check -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown --tests
bindgen=$(step browser-tools bash scripts/ensure-wasm-bindgen.sh)
bindgen=$(step browser-tool-path command -v "$bindgen")
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="$(dirname "$bindgen")/wasm-bindgen-test-runner"
step browser-host cargo test -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown
