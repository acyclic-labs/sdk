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
step host-compile bazel build --remote_download_toplevel //wasm:cloudflare-conformance
bazel_bin=$(bazel info bazel-bin) || exit $?
generated=rust/crates/conformance/cloudflare/generated
step host-facade-directory mkdir -p "$generated"
step host-facade cp -Rf "$bazel_bin/wasm/cloudflare-conformance/." "$generated/"
