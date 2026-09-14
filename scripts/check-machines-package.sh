#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output=""
if [[ "$#" -eq 1 ]]; then
  output="$1"
  if [[ "$output" != /* ]]; then
    output="$root/$output"
  fi
  [[ ! -e "$output" && ! -L "$output" ]] || {
    echo "package output must be absent: $output" >&2
    exit 2
  }
elif [[ "$#" -ne 0 ]]; then
  echo "usage: check-machines-package.sh [OUTPUT]" >&2
  exit 2
fi

if command -v wslpath >/dev/null 2>&1; then
  windows_temp="$(cmd.exe /d /c echo %TEMP% | tr -d '\r')"
  work="$(mktemp -d "$(wslpath -u "$windows_temp")/sdk-machines-package.XXXXXXXX")"
else
  work="$(mktemp -d)"
fi
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

gzip_version="$(gzip --version)"
grep -Fq 'Free Software Foundation' <<<"$gzip_version"
tar_version="$(tar --version)"
grep -Fq '(GNU tar)' <<<"$tar_version"

strict_archive() {
  gzip -t -- "$1"
  tar -tzf "$1" >/dev/null
}

cd "$root"
source_root="$work/source"
test_root="$work/test"
mkdir -p "$source_root" "$test_root"
git archive HEAD | tar -x -C "$source_root"

cargo_bin="cargo"
source_manifest="$source_root/Cargo.toml"
package_manifest="$root/Cargo.toml"
package_target="$work/package-target"
package_target_argument="$package_target"
if command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
  source_manifest="$(wslpath -w "$source_manifest")"
  package_manifest="$(wslpath -w "$package_manifest")"
  package_target_argument="$(wslpath -w "$package_target")"
fi
version="$("$cargo_bin" metadata --no-deps --format-version 1 --manifest-path "$source_manifest" | python3 -c 'import json,sys; print(next(package["version"] for package in json.load(sys.stdin)["packages"] if package["name"] == "acyclic-machines"))')"
"$cargo_bin" test --locked -p acyclic-machines -p acyclic-harness-machines --manifest-path "$source_manifest" --target-dir "$package_target_argument"
git diff --quiet
git diff --cached --quiet
"$cargo_bin" package --locked --no-verify -p acyclic-machines --manifest-path "$package_manifest" --target-dir "$package_target_argument"
crate="$package_target/package/acyclic-machines-${version}.crate"
strict_archive "$crate"
tar -xOf "$crate" "acyclic-machines-${version}/.cargo_vcs_info.json" |
  python3 -c 'import json,sys; record=json.load(sys.stdin); assert record == {"git": {"sha1": sys.argv[1]}, "path_in_vcs": "rust/crates/machines"}' "$(git rev-parse HEAD)"

tar -xzf "$crate" -C "$test_root"
test_manifest="$test_root/acyclic-machines-${version}/Cargo.toml"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  test_manifest="$(wslpath -w "$test_manifest")"
fi
"$cargo_bin" check --manifest-path "$test_manifest" --target-dir "$package_target_argument"

if [[ -n "$output" ]]; then
  mkdir -p "$(dirname "$output")"
  mkdir "$output"
  install -m 0644 "$crate" "$output/"
  staged="$output/$(basename "$crate")"
  cmp --silent "$crate" "$staged"
  strict_archive "$staged"
  (cd "$output" && sha256sum "$(basename "$crate")" > SHA256SUMS && sha256sum --strict --check SHA256SUMS)
fi
