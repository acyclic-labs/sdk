#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/qualify-actor-runtime.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
step compile bash scripts/check-actor-runtime.sh
step actors cargo test -p acyclic-actors --locked
step agents cargo test -p acyclic-harness-core --features filesystem --test agent-model --locked
step hosts bash scripts/qualify-host-serving.sh
