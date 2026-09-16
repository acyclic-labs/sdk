#!/usr/bin/env bash
set -euo pipefail

version=1.4.2
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    target=bun-linux-x64
    expected=36368faef7527875d5ffa52e53cd48021741f2a83eb6208a8dd64068d422a913
    expected_binary=a83d263767d839e4d2649ca8e35d07159c7afc99afdc96d731ced29e056dda0c
    expected_binary_bytes=79500640
    ;;
  Linux:aarch64|Linux:arm64)
    target=bun-linux-aarch64
    expected=54328bbc2d9c8e0c9f892c544d66c57a83b84139e34909e5ee81758f1ac8fda7
    expected_binary=616f267a34278ff5ac282df37ffdfba1d7141f4f6926bca99af2cd6ef3ad32b1
    expected_binary_bytes=79420872
    ;;
  Darwin:x86_64)
    target=bun-darwin-x64
    expected=80520d7e17526308c9185d261679ac6d27798d3803a0e9f7ff9121ab8affb012
    expected_binary=2fa513af22ac59e03aae640cad302e73cb1ddb0f6398501e2ddccf7dcd613596
    expected_binary_bytes=69333264
    ;;
  Darwin:arm64)
    target=bun-darwin-aarch64
    expected=90987a3a16d7db556d886ac3d551e7b6d3edf0a1cf43acaed622e8676be1d12f
    expected_binary=35d20dd0263e5c950194434b925454fdfa9ba6e4467da960410fa05b08a7a5b5
    expected_binary_bytes=61884464
    ;;
  *)
    echo "unsupported Bun host: $(uname -s):$(uname -m)" >&2
    return 1
    ;;
esac

: "${TOOLS_DIR:?TOOLS_DIR must identify the architecture-scoped CI tool cache}"
directory="$TOOLS_DIR/bun/$version/$target"
binary="$directory/bun"
digest() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 "$1" | cut -d ' ' -f 1
  fi
}
file_bytes() {
  if [[ "$(uname -s)" == Darwin ]]; then
    stat -f '%z' "$1"
  else
    stat -c '%s' "$1"
  fi
}
if [[ ! -x "$binary" || "$(file_bytes "$binary")" != "$expected_binary_bytes" || "$(digest "$binary")" != "$expected_binary" ]]; then
  command -v curl >/dev/null
  command -v unzip >/dev/null
  temporary="$(mktemp -d)"
  archive="$temporary/$target.zip"
  curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --max-filesize 134217728 \
    "https://github.com/oven-sh/bun/releases/download/bun-v$version/$target.zip" \
    --output "$archive"
  [[ "$(digest "$archive")" == "$expected" ]]
  unzip -q "$archive" -d "$temporary/extracted"
  mkdir -p "$directory"
  install -m 0755 "$temporary/extracted/$target/bun" "$binary.tmp"
  mv "$binary.tmp" "$binary"
  rm -rf "$temporary"
fi
[[ "$(file_bytes "$binary")" == "$expected_binary_bytes" ]]
[[ "$(digest "$binary")" == "$expected_binary" ]]

export PATH="$directory:$PATH"
export BUN_INSTALL_CACHE_DIR="${BUN_INSTALL_CACHE_DIR:-$TOOLS_DIR/bun/install-cache}"
mkdir -p "$BUN_INSTALL_CACHE_DIR"
[[ "$(bun --version)" == "$version" ]]
