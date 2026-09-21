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
cargo package --locked --all-features --no-verify --allow-dirty --target-dir "$package_target" \
  -p acyclic-native-runtime -p acyclic-objects -p acyclic-stream -p acyclic-fs
archives="$(printf '%s' "$metadata" | bun -e '
const metadata = await Bun.stdin.json();
for (const name of ["acyclic-native-runtime", "acyclic-objects", "acyclic-stream", "acyclic-fs"]) {
  const packages = metadata.packages.filter(item => item.name === name);
  if (packages.length !== 1) throw new Error(`ambiguous package ${name}`);
  console.log(`${name}-${packages[0].version}.crate`);
}')"
runtime_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-native-runtime").version)')"
objects_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-objects").version)')"
stream_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-stream").version)')"
fs_version="$(printf '%s' "$metadata" | bun -e 'const m=await Bun.stdin.json(); console.log(m.packages.find(p=>p.name==="acyclic-fs").version)')"
mkdir "$work/crates"
while IFS= read -r archive; do
  tar -xf "$package_target/package/$archive" -C "$work/crates"
done <<< "$archives"
mkdir -p "$work/crates/.cargo"
runtime_path="$work/crates/acyclic-native-runtime-$runtime_version"
objects_path="$work/crates/acyclic-objects-$objects_version"
stream_path="$work/crates/acyclic-stream-$stream_version"
cat >"$work/crates/.cargo/config.toml" <<EOF
[patch.crates-io]
acyclic-native-runtime = { path = "$runtime_path" }
acyclic-objects = { path = "$objects_path" }
acyclic-stream = { path = "$stream_path" }
EOF
(
  cd "$work/crates/acyclic-fs-$fs_version"
  cargo test --all-features --offline --target-dir "$work/verify-target" --no-run
)
mkdir -p "$output"
install -m 0644 "$work/acyclic-fs.tgz" "$output/acyclic-fs.tgz"
while IFS= read -r archive; do
  install -m 0644 "$package_target/package/$archive" "$output/"
done <<< "$archives"
cd "$output"
sha256sum acyclic-fs.tgz acyclic-*.crate > SHA256SUMS
git -C "$root" rev-parse --verify HEAD > SOURCE_COMMIT

package_root="$(dirname "$output")"
bash "$root/scripts/check-typescript-packages.sh" \
  "$package_root/typescript" \
  "$inference_evidence" \
  "$output/acyclic-fs.tgz" \
  "$package_root/harness/acyclic-harness.tgz"
