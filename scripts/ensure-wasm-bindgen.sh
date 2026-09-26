#!/usr/bin/env bash
set -euo pipefail

# Print the native executable path for the CLI version matching wasm-bindgen.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_platform="$(bun -e 'process.stdout.write(process.platform)')"
cargo_bin=cargo
rustup_bin=rustup
rustc_bin=rustc
bindgen_bin=wasm-bindgen
if [[ "$bun_platform" == win32 ]] && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin=cargo.exe
  rustup_bin=rustup.exe
  rustc_bin=rustc.exe
  bindgen_bin=wasm-bindgen.exe
fi
"$BASH" "$root/scripts/ensure-rust-target.sh" wasm32-unknown-unknown "$rustup_bin" "$rustc_bin" >&2

if [[ "$("$bindgen_bin" --version 2>/dev/null || true)" != 'wasm-bindgen 0.2.117' ]]; then
  if [[ "$bun_platform" != win32 && "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]]; then
    tools_dir="${TOOLS_DIR:-${TMPDIR:-/tmp}/acyclic-sdk-tools}"
    archive="$tools_dir/wasm-bindgen-0.2.117-x86_64-unknown-linux-musl.tar.gz"
    executable="$tools_dir/wasm-bindgen-0.2.117/wasm-bindgen"
    mkdir -p "$tools_dir"
    if [[ ! -f "$archive" ]] ||
      ! echo "97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240  $archive" | sha256sum --check --status; then
      temporary_archive="$(mktemp "${archive}.XXXXXXXX")"
      if ! curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
        --max-time 30 \
        https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.117/wasm-bindgen-0.2.117-x86_64-unknown-linux-musl.tar.gz \
        --output "$temporary_archive" ||
        ! echo "97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240  $temporary_archive" | sha256sum --check --status; then
        rm -f -- "$temporary_archive"
        exit 1
      fi
      mv -- "$temporary_archive" "$archive"
    fi
    if [[ "$("$executable" --version 2>/dev/null || true)" != 'wasm-bindgen 0.2.117' ]]; then
      mkdir -p "$(dirname "$executable")"
      tar --extract --gzip --file "$archive" --directory "$(dirname "$executable")" --strip-components=1
    fi
    bindgen_bin="$executable"
  else
    tools_dir="${TOOLS_DIR:-${TMPDIR:-/tmp}/acyclic-sdk-tools}"
    cargo_root="$tools_dir/cargo"
    cargo_root_arg="$cargo_root"
    if [[ "$bun_platform" == win32 ]] && command -v cygpath >/dev/null 2>&1; then
      cargo_root_arg="$(cygpath -w "$cargo_root")"
    elif [[ "$bun_platform" == win32 ]] && command -v wslpath >/dev/null 2>&1; then
      cargo_root_arg="$(wslpath -w "$cargo_root")"
    fi
    "$cargo_bin" install --locked wasm-bindgen-cli --version 0.2.117 --root "$cargo_root_arg" >&2
    bindgen_bin="$cargo_root/bin/wasm-bindgen"
    [[ "$bun_platform" == win32 ]] && bindgen_bin="${bindgen_bin}.exe"
  fi
fi
[[ "$("$bindgen_bin" --version)" == 'wasm-bindgen 0.2.117' ]]
if [[ "$bun_platform" == win32 && "$bindgen_bin" == /* ]]; then
  if command -v cygpath >/dev/null 2>&1; then
    bindgen_bin="$(cygpath -w "$bindgen_bin")"
  elif command -v wslpath >/dev/null 2>&1; then
    bindgen_bin="$(wslpath -w "$bindgen_bin")"
  fi
fi
printf '%s\n' "$bindgen_bin"
