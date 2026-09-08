#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-harness-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_platform="$(bun -e 'process.stdout.write(process.platform)')"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  windows_temp="$(cmd.exe /d /c echo %TEMP% | tr -d '\r')"
  work_parent="$(wslpath -u "$windows_temp")"
  work="$(mktemp -d "$work_parent/sdk-harness-package.XXXXXXXX")"
else
  work="$(mktemp -d -t sdk-harness-package.XXXXXXXX)"
fi
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
archive="$work/acyclic-harness.tgz"
bun_archive="$archive"
bun_archive_url="$archive"
if [[ "$bun_platform" == "win32" ]] && command -v cygpath >/dev/null 2>&1; then
  bun_archive="$(cygpath -w "$archive")"
  bun_archive_url="$(cygpath -m "$archive")"
elif [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1; then
  bun_archive="$(wslpath -w "$archive")"
  bun_archive_url="$(wslpath -m "$archive")"
fi

cd "$root"
cargo_bin="cargo"
rustup_bin="rustup"
wasm_bindgen_bin="wasm-bindgen"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
  rustup_bin="rustup.exe"
  wasm_bindgen_bin="wasm-bindgen.exe"
fi
if [[ -n "${CARGO_HOME:-}" ]]; then
  wasm_bindgen_bin="$CARGO_HOME/bin/wasm-bindgen"
  [[ "$bun_platform" == "win32" ]] && wasm_bindgen_bin="${wasm_bindgen_bin}.exe"
fi
"$rustup_bin" target add wasm32-unknown-unknown
if [[ "$("$wasm_bindgen_bin" --version 2>/dev/null || true)" != "wasm-bindgen 0.2.117" ]]; then
  "$cargo_bin" install --locked wasm-bindgen-cli --version 0.2.117
fi
export ACYCLIC_CARGO_BIN="$cargo_bin"
export ACYCLIC_WASM_BINDGEN_BIN="$wasm_bindgen_bin"
bun scripts/check-metadata.mjs
bun run --filter '@acyclic/harness' build
cd typescript/packages/harness
bun pm pack --ignore-scripts --filename "$bun_archive" --quiet

mkdir "$work/consumer"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic/harness":"file:$bun_archive_url","@bufbuild/protobuf":"2.14.1","fake-indexeddb":"6.2.4"}}
EOF
cd "$work/consumer"
bun install --ignore-scripts
mkdir -p src test generated/wasm
install -m 0644 "$root/scripts/fixtures/installed-harness/src/index.js" src/index.js
install -m 0644 "$root/scripts/fixtures/installed-harness/src/proto.js" src/proto.js
install -m 0644 "$root/scripts/fixtures/installed-harness/test/"*.test.ts test/
install -m 0644 "$root/typescript/packages/harness/test/"*.test.ts test/
install -m 0644 "$root/conformance/vectors/harness/native-wasm-event-v1.json" native-wasm-event-v1.json
install -m 0644 node_modules/@acyclic/harness/generated/wasm/acyclic_harness_wasm_bg.wasm \
  generated/wasm/acyclic_harness_wasm_bg.wasm
bun test test 2>&1 | tee "$work/typescript-package-test.log"

cd "$root"
# Stage the exact public dependency closure. Harness cannot be registry-verified
# until Stream is published, so test the extracted archives together and keep the
# release order explicit.
metadata="$("$cargo_bin" metadata --locked --no-deps --format-version 1)"
stream_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-stream").version)')"
harness_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-harness").version)')"
package_target="$work/package-target"
cargo_package_target="$package_target"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  cargo_package_target="$(wslpath -w "$cargo_package_target")"
fi
"$cargo_bin" package --locked --no-verify --allow-dirty --target-dir "$cargo_package_target" \
  -p acyclic-stream -p acyclic-harness
stream_crate="$package_target/package/acyclic-stream-$stream_version.crate"
harness_crate="$package_target/package/acyclic-harness-$harness_version.crate"

mkdir "$work/crates"
tar -xf "$stream_crate" -C "$work/crates"
tar -xf "$harness_crate" -C "$work/crates"
mkdir -p "$work/crates/.cargo"
stream_patch_path="$work/crates/acyclic-stream-$stream_version"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  stream_patch_path="$(wslpath -m "$stream_patch_path")"
fi
cat >"$work/crates/.cargo/config.toml" <<EOF
[patch.crates-io]
acyclic-stream = { path = "$stream_patch_path" }
EOF
cd "$work/crates"
"$cargo_bin" test --manifest-path "acyclic-harness-$harness_version/Cargo.toml" --all-features --offline \
  -- --test-threads=1 2>&1 | tee "$work/rust-package-test.log"

mkdir -p "$output"
install -m 0644 "$archive" "$output/"
install -m 0644 "$stream_crate" "$harness_crate" "$output/"
cmp --silent "$archive" "$output/acyclic-harness.tgz"
cmp --silent "$stream_crate" "$output/acyclic-stream-$stream_version.crate"
cmp --silent "$harness_crate" "$output/acyclic-harness-$harness_version.crate"
normalizer="$root/scripts/normalize-harness-evidence.mjs"
rust_log="$work/rust-package-test.log"
typescript_log="$work/typescript-package-test.log"
evidence_output="$output/CONFORMANCE-EVIDENCE.json"
evidence_artifacts=(
  "$output/acyclic-harness.tgz"
  "$output/acyclic-stream-$stream_version.crate"
  "$output/acyclic-harness-$harness_version.crate"
)
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1; then
  normalizer="$(wslpath -w "$normalizer")"
  rust_log="$(wslpath -w "$rust_log")"
  typescript_log="$(wslpath -w "$typescript_log")"
  evidence_output="$(wslpath -w "$evidence_output")"
  for index in "${!evidence_artifacts[@]}"; do
    evidence_artifacts[$index]="$(wslpath -w "${evidence_artifacts[$index]}")"
  done
fi
bun "$normalizer" "$rust_log" "$typescript_log" "$evidence_output" "${evidence_artifacts[@]}"
repeat_evidence="$work/CONFORMANCE-EVIDENCE.repeat.json"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1; then
  repeat_evidence="$(wslpath -w "$repeat_evidence")"
fi
bun "$normalizer" "$rust_log" "$typescript_log" "$repeat_evidence" "${evidence_artifacts[@]}"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1; then
  repeat_evidence="$(wslpath -u "$repeat_evidence")"
fi
cmp --silent "$output/CONFORMANCE-EVIDENCE.json" "$repeat_evidence"
cd "$output"
sha256sum acyclic-harness.tgz acyclic-*.crate CONFORMANCE-EVIDENCE.json > SHA256SUMS
