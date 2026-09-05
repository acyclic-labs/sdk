#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-filesystem-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d -t sdk-fs-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

cd "$root/typescript/packages/filesystem"
# The caller already compiled and tested these exact files. Never rebuild during packaging.
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
mkdir -p "$output"
install -m 0644 "$work/acyclic-fs.tgz" "$output/acyclic-fs.tgz"
cd "$output"
sha256sum acyclic-fs.tgz > SHA256SUMS
