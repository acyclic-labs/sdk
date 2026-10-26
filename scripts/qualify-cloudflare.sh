#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.." || {
  printf 'STEP cloudflare-workspace failed; recovery: run bash scripts/qualify-cloudflare.sh from the SDK checkout\n' >&2
  exit 1
}
step() {
  local name=$1
  shift
  "$@"
  local status=$?
  if [ "$status" -ne 0 ]; then
    printf 'STEP %s failed (exit %s); recovery: bash scripts/qualify-cloudflare.sh\n' "$name" "$status" >&2
    exit "$status"
  fi
}
crate=rust/crates/conformance/cloudflare
step journal-feature cargo check -p acyclic-stream --target wasm32-unknown-unknown --no-default-features --features wasm-journal --locked
step wasm-compile cargo build -p acyclic-cloudflare-conformance --target wasm32-unknown-unknown --locked
bindgen=$(bash scripts/ensure-wasm-bindgen.sh) || {
  printf 'STEP bindgen-install failed; recovery: bash scripts/ensure-wasm-bindgen.sh\n' >&2
  exit 1
}
step wasm-facade "$bindgen" --target web --out-dir "$crate/generated" "${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/debug/acyclic_cloudflare_conformance.wasm"
step typescript-dependencies bun install --frozen-lockfile --ignore-scripts
step cargo-dependencies cargo fetch --locked
step guest-facade bun scripts/build-wasm.mjs harness
step guest-module cp typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm "$crate/generated/agent_guest_bg.wasm"
step adapter-types bun x --no-install tsc --ignoreConfig --noEmit --strict --skipLibCheck --target es2022 --module nodenext --moduleResolution nodenext typescript/packages/harness/src/cloudflare.ts typescript/packages/harness/src/cloudflare-storage.ts
step worker-bundle bun build "$crate/worker.ts" --target browser --external cloudflare:workers --external '*.wasm' --outfile "$crate/worker.mjs"
step emulator-dependencies npm ci --prefix "$crate" --ignore-scripts
step workerd-suites node "$crate/run.mjs"
