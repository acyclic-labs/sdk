#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-filesystem-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d -t sdk-fs-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

cd "$root/typescript/packages/filesystem"
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
# Cargo stages this public dependency closure together, then verifies each extracted crate.
# No source-path dependency or private registry is substituted into consumer manifests.
cargo package --locked --all-features -p acyclic-objects -p acyclic-stream -p acyclic-fs
archives="$(cargo metadata --locked --no-deps --format-version 1 | bun -e '
const metadata = await Bun.stdin.json();
for (const name of ["acyclic-objects", "acyclic-stream", "acyclic-fs"]) {
  const packages = metadata.packages.filter(item => item.name === name);
  if (packages.length !== 1) throw new Error(`ambiguous package ${name}`);
  console.log(`${metadata.target_directory}/package/${name}-${packages[0].version}.crate`);
}')"
mkdir -p "$output"
install -m 0644 "$work/acyclic-fs.tgz" "$output/acyclic-fs.tgz"
while IFS= read -r archive; do
  install -m 0644 "$archive" "$output/"
done <<< "$archives"
cd "$output"
sha256sum acyclic-fs.tgz acyclic-*.crate > SHA256SUMS
