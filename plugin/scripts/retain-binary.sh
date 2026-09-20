#!/usr/bin/env bash
# Retains a qualified `acyclic` binary as a lane artifact: the exact bytes the
# acceptance suite just ran, with a SHA-256 inventory and the source commit,
# so the publish workflow can ship them without a rebuild.
#
#   retain-binary.sh <binary> <output-dir>
#
# Refuses to overwrite: an existing output directory means two lanes or two
# steps disagree about who produced this target.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: retain-binary.sh <binary> <output-dir>" >&2; exit 2; }
binary="$1"; out="$2"
[ -f "$binary" ] || { echo "retain-binary: no binary at $binary" >&2; exit 1; }
# Two crates in the sdk workspace emit a binary called `acyclic` (this
# plugin and the sdk's own acyclic-cli), and cargo lets the last build win,
# so prove this is the plugin's before it becomes a release input: it must
# report the product name and this tree's crate version.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=plugin/scripts/product.sh
source "$here/scripts/product.sh"
expected="$PRODUCT_NAME $(awk -F'"' '/^version = /{print $2; exit}' "$here/crates/acyclic/Cargo.toml")"
reported="$("$binary" --version 2>/dev/null || true)"
case "$binary" in
  */x86_64-apple-darwin/*) reported="$(arch -x86_64 "$binary" --version 2>/dev/null || true)" ;;
esac
[ "$reported" = "$expected" ] || {
  echo "retain-binary: $binary reports '$reported', expected '$expected' (output filename collision?)" >&2
  exit 1
}
[ ! -e "$out" ] || { echo "retain-binary: $out already exists" >&2; exit 1; }
mkdir -p "$out"
name="$(basename "$binary")"
cp "$binary" "$out/$name"
chmod 0755 "$out/$name"
# GNU coreutils on the Linux and Windows (Git Bash) lanes; stock macOS has
# shasum. Both write the `<hash>  <name>` line sha256sum --check reads.
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$out" && sha256sum "$name" > SHA256SUMS)
else
  (cd "$out" && shasum -a 256 "$name" > SHA256SUMS)
fi
commit="${CI_HEAD_SHA:-$(git rev-parse HEAD)}"
printf '%s\n' "$commit" > "$out/SOURCE_COMMIT"
echo "retained $name for $commit:"
cat "$out/SHA256SUMS"
