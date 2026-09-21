#!/usr/bin/env bash
set -euo pipefail

[[ $# == 2 ]] || { echo 'usage: stage-npm-package.sh SOURCE TARGET' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
source="$(cd "$1" && pwd -P)"
target=$2
case "$source" in
  "$root/typescript/packages/"*) ;;
  *) echo 'npm package source is outside the TypeScript workspace' >&2; exit 2 ;;
esac
[[ ! -e "$target" && ! -L "$target" ]] || { echo 'npm package stage must be absent' >&2; exit 2; }
mkdir -p "$target"
tar --create --file - --directory "$source" \
  --exclude='./node_modules' --exclude='./node_modules/**' \
  --exclude='./*.tgz' . | tar --extract --file - --directory "$target"
install -m 0644 "$root/CHANGELOG.md" "$target/CHANGELOG.md"
[[ -f "$target/package.json" && -f "$target/README.md" && -f "$target/CHANGELOG.md" ]] || {
  echo 'staged npm package lacks release metadata' >&2
  exit 2
}
