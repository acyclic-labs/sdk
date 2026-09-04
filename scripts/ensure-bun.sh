#!/usr/bin/env bash
set -euo pipefail

version=1.3.14
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    target=bun-linux-x64
    expected=951ee2aee855f08595aeec6225226a298d3fea83a3dcd6465c09cbccdf7e848f
    expected_binary=9fd36f87e4b90b07632b987a2e4ec81ca15a62c81bf983190cea6d715be2ad74
    expected_binary_bytes=92752752
    ;;
  Linux:aarch64|Linux:arm64)
    target=bun-linux-aarch64
    expected=a27ffb63a8310375836e0d6f668ae17fa8d8d18b88c37c821c65331973a19a3b
    expected_binary=37141662ebed915a2ab89313156e455e2a1374395f5f6760d06407f49406f086
    expected_binary_bytes=91801560
    ;;
  Darwin:x86_64)
    target=bun-darwin-x64
    expected=4183df3374623e5bab315c547cfa0974533cd457d86b73b639f7a87974cd6633
    expected_binary=ea2f223e94bb2f4bf3050895113c3cf346438f6fa0501c8532284e063f72f7a0
    expected_binary_bytes=69173328
    ;;
  Darwin:arm64)
    target=bun-darwin-aarch64
    expected=d8b96221828ad6f97ac7ac0ab7e95872341af763001e8803e8267652c2652620
    expected_binary=e0c90ec15d33363e6b70713d56bc3b2c7585c17f40a0fe0f8fd9305901d4e233
    expected_binary_bytes=63096576
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
export BUN_INSTALL_CACHE_DIR="$TOOLS_DIR/bun/install-cache"
mkdir -p "$BUN_INSTALL_CACHE_DIR"
[[ "$(bun --version)" == "$version" ]]
