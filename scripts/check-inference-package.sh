#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

work="$(mktemp -d)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
source_root="$work/source"
test_root="$work/test"
mkdir -p "$source_root" "$test_root"
git archive HEAD | tar -x -C "$source_root"

version="$(cargo metadata --no-deps --format-version 1 --manifest-path "$source_root/Cargo.toml" | python3 -c 'import json,sys; print(next(package["version"] for package in json.load(sys.stdin)["packages"] if package["name"] == "inference-sdk"))')"
cargo package --locked --no-verify -p inference-sdk --manifest-path "$source_root/Cargo.toml"
crate="$source_root/target/package/inference-sdk-${version}.crate"
actual="$(sha256sum "$crate" | cut -d ' ' -f 1)"
expected="$(python3 - "$version" <<'PY'
import json, pathlib, sys

entries = [
    json.loads(line)
    for line in pathlib.Path("registry/in/fe/inference-sdk").read_text(encoding="utf-8").splitlines()
    if line
]
matches = [entry for entry in entries if entry["vers"] == sys.argv[1]]
if len(matches) != 1:
    raise SystemExit("sparse index must contain exactly one current inference-sdk release")
print(matches[0]["cksum"])
PY
)"
test "$actual" = "$expected"

tar -xf "$crate" -C "$test_root"
cargo test --manifest-path "$test_root/inference-sdk-${version}/Cargo.toml"

if [[ "$#" -eq 1 ]]; then
  output="$1"
  mkdir -p "$output"
  install -m 0644 "$crate" "$output/"
  printf '%s  %s\n' "$actual" "$(basename "$crate")" > "$output/SHA256SUMS"
elif [[ "$#" -ne 0 ]]; then
  echo "usage: check-inference-package.sh [OUTPUT]" >&2
  exit 2
fi
