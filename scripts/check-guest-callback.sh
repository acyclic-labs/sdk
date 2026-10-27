#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/check-guest-callback.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
step dependencies bun install --frozen-lockfile --ignore-scripts
step cargo-dependencies cargo fetch --locked
step guest-facade bun scripts/build-wasm.mjs harness
step guest-callback-types bun x --no-install tsc --ignoreConfig --noEmit --strict --skipLibCheck --target es2022 --module nodenext --moduleResolution nodenext typescript/packages/harness/test/guest-hosting-types.ts
