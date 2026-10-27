#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/qualify-host-serving.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
step agent-host bash scripts/qualify-agent-host.sh
step cloudflare bash scripts/qualify-cloudflare.sh
