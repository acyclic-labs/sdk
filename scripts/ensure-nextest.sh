#!/usr/bin/env bash
set -euo pipefail

# The one installer of the pinned cargo-nextest for every CI host: Linux,
# macOS and Windows (Git Bash). It keeps the verified release archive and the
# executable under $TOOLS_DIR, verifies the archive's SHA-256 before every
# extraction, and prints the directory holding cargo-nextest in the host's
# native path form, ready for PATH or $GITHUB_PATH.
version=0.9.146
: "${TOOLS_DIR:?TOOLS_DIR must name the CI tool cache}"
tools="$TOOLS_DIR"
executable=cargo-nextest
native_path() { printf '%s\n' "$1"; }
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    target=x86_64-unknown-linux-gnu
    expected=682c21b777c333e96fd532e114d3a5a894e0729ab88d94c0a9f20f8419695428 ;;
  Linux:aarch64|Linux:arm64)
    target=aarch64-unknown-linux-gnu
    expected=b2e33d7c72de7ade0ff7b3a948ac37516b24f8a836b7a8870c1f634a94be9de9 ;;
  Darwin:*)
    target=universal-apple-darwin
    expected=39785160b3c2f6ed9a765049cf4fa79f3b39aa02eb7598a5a0e2a1a0b9ffb9a8 ;;
  MINGW*:x86_64|MSYS*:x86_64|CYGWIN*:x86_64)
    target=x86_64-pc-windows-msvc
    expected=809b01f2c94d031ab6cf5f4d4a4f477c8f51b69c96ab3cb2fe4db7ecb222d0d3
    executable=cargo-nextest.exe
    # GNU tar reads a drive letter as a remote host, so work in POSIX form.
    tools="$(cygpath -u "$TOOLS_DIR")"
    native_path() { cygpath -w "$1"; } ;;
  *)
    echo "unsupported cargo-nextest host: $(uname -s):$(uname -m)" >&2
    exit 1 ;;
esac

bin="$tools/cargo/bin"
archive="$tools/cargo-nextest-$version-$target.tar.gz"
url="https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-$version/cargo-nextest-$version-$target.tar.gz"
release="^cargo-nextest ${version//./[.]}( |\$)"
recovery="step=install pinned cargo-nextest $version; recovery: TOOLS_DIR=$TOOLS_DIR bash scripts/ensure-nextest.sh"

digest() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 "$1" | cut -d ' ' -f 1
  fi
}
# Accepts exactly this release: the first line of --version names it, followed
# by build details or nothing, so 0.9.1461 and 0.9.146-dev are refused.
installed() {
  local reported
  [[ -x "$bin/$executable" ]] || return 1
  reported="$("$bin/$executable" nextest --version 2>/dev/null)" || return 1
  reported="${reported%%$'\n'*}"
  [[ "${reported%$'\r'}" =~ $release ]]
}

if ! installed; then
  mkdir -p "$bin"
  if [[ ! -f "$archive" || "$(digest "$archive")" != "$expected" ]]; then
    temporary="$(mktemp "$archive.XXXXXXXX")"
    if ! {
      if [[ -n "${PINNED_ASSET_FETCHER:-}" ]]; then
        "$PINNED_ASSET_FETCHER" "$expected" "$url" "$(native_path "$temporary")" >&2
      else
        curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
          --max-time 30 "$url" --output "$temporary"
      fi
    } || [[ "$(digest "$temporary")" != "$expected" ]]; then
      rm -f -- "$temporary"
      printf 'cargo-nextest archive refused (fetch failed or SHA-256 differs from %s): %s\n%s\n' \
        "$expected" "$url" "$recovery" >&2
      exit 1
    fi
    mv -- "$temporary" "$archive"
  fi
  tar --extract --gzip --file "$archive" --directory "$bin" "$executable"
  if ! installed; then
    printf 'extracted %s does not report cargo-nextest %s\n%s\n' "$bin/$executable" "$version" "$recovery" >&2
    exit 1
  fi
fi
native_path "$bin"
