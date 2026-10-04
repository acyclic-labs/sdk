#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-filesystem-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
inference_evidence="$(dirname "$output")/inference/acyclic-inference.tgz"
[[ -f "$inference_evidence" ]] || { echo 'filesystem qualification requires the preceding inference package artifact' >&2; exit 2; }
work="$(mktemp -d -t sdk-fs-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

npm_stage="$work/npm-package"
bash "$root/scripts/stage-npm-package.sh" "$root/typescript/packages/filesystem" "$npm_stage"
cd "$npm_stage"
# The caller already compiled and tested these JavaScript/WASM files; pack those exact bytes.
bun pm pack --ignore-scripts --filename "$work/acyclic-fs.tgz" --quiet
tar -xzf "$work/acyclic-fs.tgz" -C "$work"
mkdir "$work/package/test"
cp test/node-memory.mjs test/workspace-composition.mjs "$work/package/test/"
cd "$work/package"
# The archive is tested as an isolated npm consumer. Install its declared
# runtime dependencies before importing the generated adapters; the repository
# workspace's node_modules is intentionally outside this extracted package.
bun install --ignore-scripts --no-save --no-progress
timeout 30s bun test/node-memory.mjs
# Prove that the isolated consumer used the archived WASM, not a workspace fallback.
mv generated/wasm/acyclic_fs_wasm_bg.wasm "$work/withheld.wasm"
if timeout 30s bun test/node-memory.mjs >"$work/missing-wasm.log" 2>&1; then
  echo 'packaged consumer unexpectedly ran without its WASM' >&2
  exit 1
fi
grep -q 'ENOENT' "$work/missing-wasm.log"

cd "$root"
# Stage the exact dependency closure. The higher crates cannot be registry-verified
# until the native runtime is published, so verify the extracted archives together.
metadata="$(cargo metadata --locked --no-deps --format-version 1)"
package_target="$work/package-target"
package_config=()
for name in acyclic-native-runtime acyclic-objects acyclic-stream \
  acyclic-sdk-contract-options acyclic-sdk-contract-wire; do
  package_dir="${name#acyclic-}"
  package_source="$root/rust/crates/$package_dir"
  [[ -d "$package_source" ]] || {
    echo "missing Rust-owned package source: $package_source" >&2
    exit 1
  }
  if [[ "$(bun -e 'process.stdout.write(process.platform)')" == "win32" ]]; then
    package_source="$(bash "$root/scripts/native-tool-path.sh" "$package_source")"
  fi
  package_config+=(--config "patch.crates-io.$name.path=\"$package_source\"")
done
cargo package --offline --locked --all-features --no-verify --allow-dirty --target-dir "$package_target" \
  "${package_config[@]}" \
  -p acyclic-native-runtime -p acyclic-objects -p acyclic-stream \
  -p acyclic-sdk-contract-options -p acyclic-sdk-contract-wire -p acyclic-fs
archives="$(printf '%s' "$metadata" | bun -e '
const metadata = await Bun.stdin.json();
for (const name of ["acyclic-native-runtime", "acyclic-objects", "acyclic-stream", "acyclic-sdk-contract-options", "acyclic-sdk-contract-wire", "acyclic-fs"]) {
  const packages = metadata.packages.filter(item => item.name === name);
  if (packages.length !== 1) throw new Error(`ambiguous package ${name}`);
  console.log(`${name}-${packages[0].version}.crate`);
}')"
runtime_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-native-runtime").version)')"
objects_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-objects").version)')"
stream_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-stream").version)')"
options_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-sdk-contract-options").version)')"
wire_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-sdk-contract-wire").version)')"
fs_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-fs").version)')"
mkdir "$work/crates"
while IFS= read -r archive; do
  tar -xf "$package_target/package/$archive" -C "$work/crates"
done <<< "$archives"
mkdir -p "$work/crates/.cargo"
runtime_path="$work/crates/acyclic-native-runtime-$runtime_version"
objects_path="$work/crates/acyclic-objects-$objects_version"
stream_path="$work/crates/acyclic-stream-$stream_version"
options_path="$work/crates/acyclic-sdk-contract-options-$options_version"
wire_path="$work/crates/acyclic-sdk-contract-wire-$wire_version"
if [[ "$(bun -e 'process.stdout.write(process.platform)')" == "win32" ]]; then
  runtime_path="$(bash "$root/scripts/native-tool-path.sh" "$runtime_path")"
  objects_path="$(bash "$root/scripts/native-tool-path.sh" "$objects_path")"
  stream_path="$(bash "$root/scripts/native-tool-path.sh" "$stream_path")"
  options_path="$(bash "$root/scripts/native-tool-path.sh" "$options_path")"
  wire_path="$(bash "$root/scripts/native-tool-path.sh" "$wire_path")"
fi
cat >"$work/crates/.cargo/config.toml" <<EOF
[patch.crates-io]
acyclic-native-runtime = { path = "$runtime_path" }
acyclic-objects = { path = "$objects_path" }
acyclic-stream = { path = "$stream_path" }
acyclic-sdk-contract-options = { path = "$options_path" }
acyclic-sdk-contract-wire = { path = "$wire_path" }
EOF
# sdk-contract-wire intentionally keeps the canonical descriptor inputs at
# source-relative paths. Package those exact Rust-owned bytes into a versioned
# archive and extract them beside the installed wire crate. The extracted build
# must never resolve a descriptor from this producer checkout.
wire_inputs="$work/wire-inputs/acyclic-sdk-contract-wire-inputs-$wire_version"
mkdir -p "$wire_inputs"
stage_wire_input() {
  source="$root/$1"
  destination="$wire_inputs/$2"
  [[ -f "$source" ]] || { echo "missing canonical wire input: $source" >&2; exit 1; }
  mkdir -p "$(dirname "$destination")"
  install -m 0644 "$source" "$destination"
}
stage_wire_input rust/crates/actors/src/generated/acyclic-actors-v1.bin actors/src/generated/acyclic-actors-v1.bin
stage_wire_input rust/crates/workers/src/generated/acyclic-workers-v1.bin workers/src/generated/acyclic-workers-v1.bin
stage_wire_input rust/crates/objects/src/generated/acyclic-objects-v2.bin objects/src/generated/acyclic-objects-v2.bin
stage_wire_input rust/crates/stream/proto/stream/v2/stream_descriptor.bin stream/proto/stream/v2/stream_descriptor.bin
stage_wire_input rust/crates/inference/inference_descriptor.bin inference/inference_descriptor.bin
stage_wire_input rust/crates/machines/src/generated/acyclic-machines-v1.bin machines/src/generated/acyclic-machines-v1.bin
stage_wire_input rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin filesystem/src/generated/acyclic-filesystem-v2.bin
stage_wire_input rust/crates/harness/src/generated/harness-archived-v2.bin harness/src/generated/harness-archived-v2.bin
wire_input_archive="$work/acyclic-sdk-contract-wire-inputs-$wire_version.tar.gz"
tar -czf "$wire_input_archive" -C "$work/wire-inputs" "acyclic-sdk-contract-wire-inputs-$wire_version"
tar -xzf "$wire_input_archive" --strip-components=1 -C "$work/crates"
if grep -R -F -- "$root" "$work/crates" >/dev/null 2>&1; then
  echo 'archive-only Filesystem consumer contains a producer checkout path' >&2
  exit 1
fi
(
  cd "$work/crates"
  cargo test --all-features --offline --target-dir "$work/verify-target" --no-run
)
mkdir -p "$output"
install -m 0644 "$work/acyclic-fs.tgz" "$output/acyclic-fs.tgz"
while IFS= read -r archive; do
  install -m 0644 "$package_target/package/$archive" "$output/"
done <<< "$archives"
install -m 0644 "$wire_input_archive" "$output/"
cd "$output"
sha256sum acyclic-fs.tgz acyclic-*.crate acyclic-sdk-contract-wire-inputs-*.tar.gz > SHA256SUMS
git -C "$root" rev-parse --verify HEAD > SOURCE_COMMIT

package_root="$(dirname "$output")"
bash "$root/scripts/check-typescript-packages.sh" \
  "$package_root/typescript" \
  "$inference_evidence" \
  "$output/acyclic-fs.tgz" \
  "$package_root/harness/acyclic-harness.tgz"
