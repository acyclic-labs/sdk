#!/usr/bin/env bash
set -euo pipefail

# Print the native executable path for the CLI version matching wasm-bindgen.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_platform="$(bun -e 'process.stdout.write(process.platform)')"
rustup_bin=rustup
rustc_bin=rustc
bindgen_bin=wasm-bindgen
if [[ "$bun_platform" == win32 ]]; then
  bindgen_bin=wasm-bindgen.exe
  if command -v rustup.exe >/dev/null 2>&1; then
    rustup_bin=rustup.exe
    rustc_bin=rustc.exe
  fi
fi
"$BASH" "$root/scripts/ensure-rust-target.sh" wasm32-unknown-unknown "$rustup_bin" "$rustc_bin" >&2

if [[ "$("$bindgen_bin" --version 2>/dev/null || true)" != 'wasm-bindgen 0.2.117' ]]; then
  case "$bun_platform:$(bun -e 'process.stdout.write(process.arch)')" in
    linux:x64) triple=x86_64-unknown-linux-musl; digest=97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240 ;;
    linux:arm64) triple=aarch64-unknown-linux-musl; digest=a9e2c56bf58a95bdfd8f2a67f726a3d11fa873cfe984468e5bbf0e48cb7dd923 ;;
    darwin:x64) triple=x86_64-apple-darwin; digest=17d991583d7036cd773b62eb8c0f295dd31cde7bf9ab250276be56f9138723d3 ;;
    darwin:arm64) triple=aarch64-apple-darwin; digest=d7691b221163d68884ef0a893cb57d3372f1100789dbf0cc2c9260a7f253a497 ;;
    win32:x64) triple=x86_64-pc-windows-msvc; digest=54d52650adcf93e332d1d02c94c81925dfbda1f1292cecf276a0507263a4936f ;;
    *) printf 'No pinned wasm-bindgen archive for this host\n' >&2; exit 1 ;;
  esac
  tools_dir="${TOOLS_DIR:-${TMPDIR:-/tmp}/acyclic-sdk-tools}"
  distribution="wasm-bindgen-0.2.117-$triple"
  archive="$tools_dir/$distribution.tar.gz"
  executable="$tools_dir/$distribution/$bindgen_bin"
  url="https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.117/$distribution.tar.gz"
  archive_matches() {
    local observed
    if [[ "$bun_platform" == darwin ]]; then
      observed=$(shasum -a 256 < "$1") || return 1
    else
      observed=$(sha256sum < "$1") || return 1
    fi
    [[ "${observed%% *}" == "$digest" ]]
  }
  mkdir -p "$tools_dir"
  if [[ ! -f "$archive" ]] || ! archive_matches "$archive"; then
    temporary_archive=$(mktemp "${archive}.XXXXXXXX")
    if ! {
      if [[ -n "${PINNED_ASSET_FETCHER:-}" ]]; then
        "$PINNED_ASSET_FETCHER" "$digest" "$url" "$temporary_archive" >&2
      else
        curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
          --max-time 30 "$url" --output "$temporary_archive"
      fi
    } || ! archive_matches "$temporary_archive"; then
      rm -f -- "$temporary_archive"
      printf 'step=fetch pinned wasm-bindgen: %s; recovery: bash scripts/ensure-wasm-bindgen.sh\n' "$url" >&2
      exit 1
    fi
    mv -- "$temporary_archive" "$archive"
  fi
  if [[ "$("$executable" --version 2>/dev/null || true)" != 'wasm-bindgen 0.2.117' ]]; then
    mkdir -p "$(dirname "$executable")"
    tar --extract --gzip --file "$archive" --directory "$(dirname "$executable")" --strip-components=1
  fi
  bindgen_bin="$executable"
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
