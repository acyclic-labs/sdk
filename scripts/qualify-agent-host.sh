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
step browser-host bash scripts/run-browser-tests.sh cargo test --locked -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown
