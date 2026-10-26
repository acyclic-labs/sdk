#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-filesystem-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
inference_evidence="$(dirname "$output")/inference/acyclic-inference.tgz"
[[ -f "$inference_evidence" ]] || { echo 'filesystem qualification requires the preceding inference package artifact' >&2; exit 2; }
# sccache keys Rust compilations by working directory and source paths, so a
# fresh mktemp path would miss on every run. A stable per-checkout directory,
# emptied before and after each run, keeps the package isolated and cached.
work="${TMPDIR:-/tmp}/sdk-fs-package-$(printf '%s' "$root" | cksum | cut -d' ' -f1)"
rm -rf -- "$work"
(umask 077 && mkdir -- "$work")
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

npm_stage="$work/npm-package"
bash "$root/scripts/stage-npm-package.sh" "$root/typescript/packages/filesystem" "$npm_stage"
observer_stage="$work/observer-package"
bash "$root/scripts/stage-npm-package.sh" "$root/typescript/packages/actors" "$observer_stage"
(cd "$observer_stage" && bun pm pack --ignore-scripts --filename acyclic-observe.tgz --quiet)
mv "$observer_stage/acyclic-observe.tgz" "$work/acyclic-observe.tgz"
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
bun -e 'const fs = require("node:fs"); const manifest = JSON.parse(fs.readFileSync("package.json", "utf8")); manifest.overrides = { ...manifest.overrides, "@acyclic-labs/actors": "file:../acyclic-observe.tgz" }; fs.writeFileSync("package.json", JSON.stringify(manifest));'
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
closure_output="$(bun scripts/harness-package-closure.mjs acyclic-fs)"
mapfile -t closure <<< "$closure_output"
package_arguments=()
archives=()
fs_version=""
for entry in "${closure[@]}"; do
  IFS=$'\t' read -r name version <<< "$entry"
  package_arguments+=(-p "$name")
  archives+=("$name-$version.crate")
  if [[ "$name" == acyclic-fs ]]; then fs_version="$version"; fi
done
[[ -n "$fs_version" ]]
package_target="$work/package-target"
cargo package --locked --all-features --no-verify --allow-dirty --target-dir "$package_target" "${package_arguments[@]}"
mkdir "$work/crates"
for archive in "${archives[@]}"; do
  tar -xf "$package_target/package/$archive" -C "$work/crates"
done
mkdir -p "$work/crates/.cargo"
printf '[patch.crates-io]\n' >"$work/crates/.cargo/config.toml"
for entry in "${closure[@]}"; do
  IFS=$'\t' read -r name version <<< "$entry"
  if [[ "$name" == acyclic-fs ]]; then continue; fi
  patch_path="$work/crates/$name-$version"
  if [[ "$(bun -e 'process.stdout.write(process.platform)')" == win32 ]]; then
    patch_path="$(bash "$root/scripts/native-tool-path.sh" "$patch_path")"
  fi
  printf '%s = { path = "%s" }\n' "$name" "$patch_path" >>"$work/crates/.cargo/config.toml"
done
(
  cd "$work/crates/acyclic-fs-$fs_version"
  cargo test --all-features --offline --target-dir "$work/verify-target" --no-run
)
mkdir -p "$output"
install -m 0644 "$work/acyclic-fs.tgz" "$output/acyclic-fs.tgz"
for archive in "${archives[@]}"; do
  install -m 0644 "$package_target/package/$archive" "$output/"
done
cd "$output"
sha256sum acyclic-fs.tgz acyclic-*.crate > SHA256SUMS
git -C "$root" rev-parse --verify HEAD > SOURCE_COMMIT

package_root="$(dirname "$output")"
node "$root/scripts/typescript-qualification.mjs" consumer "$(command -v bun)" "$package_root/BUILD.json"
bash "$root/scripts/check-typescript-packages.sh" \
  "$package_root/typescript" \
  "$inference_evidence" \
  "$output/acyclic-fs.tgz" \
  "$package_root/harness/acyclic-harness.tgz" \
  "$package_root/BUILD.json"
