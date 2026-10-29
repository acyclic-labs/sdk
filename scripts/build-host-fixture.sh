#!/usr/bin/env bash
set -u
step() {
  local name=$1
  shift
  "$@"
  local result=$?
  if [ "$result" -ne 0 ]; then
    printf 'STEP %s failed (exit %s); recovery: bash scripts/build-host-fixture.sh\n' "$name" "$result" >&2
    exit "$result"
  fi
}
step workspace cd "$(dirname "$0")/.."
step host-compile cargo build -p acyclic-cloudflare-conformance --target wasm32-unknown-unknown --locked
bindgen=$(bash scripts/ensure-wasm-bindgen.sh) || {
  printf 'STEP host-bindgen failed; recovery: bash scripts/ensure-wasm-bindgen.sh\n' >&2
  exit 1
}
step host-facade "$bindgen" --target web --out-dir rust/crates/conformance/cloudflare/generated "${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/debug/acyclic_cloudflare_conformance.wasm"
