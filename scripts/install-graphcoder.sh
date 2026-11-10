#!/bin/sh
# The release assembler pins this installer to the coordinated release version.
set -eu
version='@VERSION@'
case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) echo 'GraphCoder supports Linux, macOS and Windows (install-graphcoder.ps1).' >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=x64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo 'GraphCoder requires x86_64 or arm64.' >&2; exit 1 ;;
esac
target="$os-$arch"
if [ "$os" = linux ]; then
  if getconf GNU_LIBC_VERSION >/dev/null 2>&1; then target="$target-gnu"; else target="$target-musl"; fi
fi
asset="graphcoder-$target"
directory=${GRAPHCODER_INSTALL_DIR:-"$HOME/.local/bin"}
mkdir -p "$directory"
staging=$(mktemp -d "$directory/.graphcoder-install.XXXXXXXX")
trap 'rm -rf "$staging"' EXIT HUP INT TERM
fetch() {
  if [ -n "${GRAPHCODER_ASSET_DIR:-}" ]; then
    cp "$GRAPHCODER_ASSET_DIR/$1" "$2"
  else
    curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
      "https://github.com/acyclic-labs/sdk/releases/download/acyclic-v$version/$1" -o "$2"
  fi
}
fetch SHA256SUMS "$staging/SHA256SUMS"
# An exact, unique entry is required; never evaluate downloaded filenames.
expected=$(awk -v name="$asset" '$2 == name { count++; value=$1 } END { if (count != 1 || length(value) != 64 || value ~ /[^0-9a-f]/) exit 1; print value }' "$staging/SHA256SUMS")
fetch "$asset" "$staging/graphcoder"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$staging/graphcoder")
else
  actual=$(shasum -a 256 "$staging/graphcoder")
fi
actual=${actual%% *}
if [ "$actual" != "$expected" ]; then echo 'GraphCoder SHA-256 mismatch.' >&2; exit 1; fi
chmod 755 "$staging/graphcoder"
actual=$("$staging/graphcoder" --version)
if [ "$actual" != "graphcoder $version" ]; then echo 'GraphCoder version mismatch.' >&2; exit 1; fi
# Same-filesystem rename preserves the previous executable on any earlier failure.
if [ -d "$directory/graphcoder" ]; then echo 'GraphCoder destination is a directory.' >&2; exit 1; fi
mv -f "$staging/graphcoder" "$directory/graphcoder"
printf 'Installed %s to %s/graphcoder\nAdd %s to PATH if needed.\n' "$actual" "$directory" "$directory"
