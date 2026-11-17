#!/usr/bin/env bash
set -euo pipefail

# Reuse the CLI selected by the build graph; no second installer or version pin.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_platform="$(bun -e 'process.stdout.write(process.platform)')"
rustup_bin=rustup
rustc_bin=rustc
bazel_bin="${ACYCLIC_BAZEL:-bazel}"
if [[ "$bun_platform" == win32 ]]; then
  command -v rustup.exe >/dev/null 2>&1 && rustup_bin=rustup.exe
  command -v rustc.exe >/dev/null 2>&1 && rustc_bin=rustc.exe
  if [[ -z "${ACYCLIC_BAZEL:-}" ]] && command -v bazel.exe >/dev/null 2>&1; then
    bazel_bin=bazel.exe
  fi
fi
"$BASH" "$root/scripts/ensure-rust-target.sh" wasm32-unknown-unknown "$rustup_bin" "$rustc_bin" >&2

cd "$root"
target=@rules_rust_wasm_bindgen//3rdparty:wasm_bindgen_cli
"$bazel_bin" build "$target" >&2
execution_root="$("$bazel_bin" info execution_root)"
executable="$("$bazel_bin" cquery "$target" --output=files)"
bindgen_bin="${execution_root%$'\r'}/${executable%$'\r'}"
if [[ "$bun_platform" == win32 && "$bindgen_bin" == /* ]]; then
  if command -v cygpath >/dev/null 2>&1; then
    bindgen_bin="$(cygpath -w "$bindgen_bin")"
  elif command -v wslpath >/dev/null 2>&1; then
    bindgen_bin="$(wslpath -w "$bindgen_bin")"
  fi
fi
printf '%s\n' "$bindgen_bin"
