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

clean_head() {
  local repository="$1"
  local expected="$2"
  [[ "$(git -C "$repository" rev-parse HEAD)" == "$expected" ]] &&
    ! git -C "$repository" ls-files -v | grep '^[a-z]' >/dev/null &&
    ! git -C "$repository" ls-files -t | grep '^S ' >/dev/null &&
    [[ -z "$(git -C "$repository" status --porcelain --untracked-files=all)" ]]
}

valid_vcs_info() {
  python3 -c 'import json,sys; expected={"git":{"sha1":sys.argv[1]},"path_in_vcs":"rust/crates/machines"}; raise SystemExit(json.load(sys.stdin) != expected)' "$1"
}

cd "$root"
head="$(git rev-parse HEAD)"
clean_head "$root" "$head"

guard_repo="$work/guard-repository"
git init --quiet "$guard_repo"
git -C "$guard_repo" config user.email test@example.invalid
git -C "$guard_repo" config user.name Test
printf 'clean\n' > "$guard_repo/fixture"
git -C "$guard_repo" add fixture
git -C "$guard_repo" commit --quiet -m initial
guard_head="$(git -C "$guard_repo" rev-parse HEAD)"
clean_head "$guard_repo" "$guard_head"
git -C "$guard_repo" update-index --assume-unchanged fixture
! clean_head "$guard_repo" "$guard_head"
git -C "$guard_repo" update-index --no-assume-unchanged fixture
git -C "$guard_repo" update-index --skip-worktree fixture
! clean_head "$guard_repo" "$guard_head"
git -C "$guard_repo" update-index --no-skip-worktree fixture
printf 'untracked\n' > "$guard_repo/untracked"
! clean_head "$guard_repo" "$guard_head"
rm "$guard_repo/untracked"
printf 'dirty\n' >> "$guard_repo/fixture"
! clean_head "$guard_repo" "$guard_head"
git -C "$guard_repo" restore fixture
printf 'staged\n' >> "$guard_repo/fixture"
git -C "$guard_repo" add fixture
! clean_head "$guard_repo" "$guard_head"
git -C "$guard_repo" commit --quiet -m changed
! clean_head "$guard_repo" "$guard_head"

printf '{"git":{"sha1":"%s"},"path_in_vcs":"rust/crates/machines"}\n' "$head" | valid_vcs_info "$head"
for invalid_vcs_info in \
  '{}' \
  '{"git":{"sha1":"wrong"},"path_in_vcs":"rust/crates/machines"}' \
  "{\"git\":{\"sha1\":\"$head\"},\"path_in_vcs\":\"wrong\"}" \
  "{\"git\":{\"sha1\":\"$head\",\"dirty\":false},\"path_in_vcs\":\"rust/crates/machines\"}"; do
  ! printf '%s\n' "$invalid_vcs_info" | valid_vcs_info "$head"
done

source_root="$work/source"
package_root="$work/package-source"
test_root="$work/test"
mkdir -p "$source_root" "$test_root"
git archive "$head" | tar -x -C "$source_root"
git clone --quiet --no-checkout -- "$root" "$package_root"
git -C "$package_root" checkout --quiet --detach "$head"
clean_head "$package_root" "$head"

cargo_bin="cargo"
source_manifest="$source_root/Cargo.toml"
package_manifest="$package_root/Cargo.toml"
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
clean_head "$root" "$head"
clean_head "$package_root" "$head"
"$cargo_bin" package --locked --no-verify -p acyclic-machines --manifest-path "$package_manifest" --target-dir "$package_target_argument"
crate="$package_target/package/acyclic-machines-${version}.crate"
strict_archive "$crate"
tar -xOf "$crate" "acyclic-machines-${version}/.cargo_vcs_info.json" |
  valid_vcs_info "$head"
clean_head "$root" "$head"
clean_head "$package_root" "$head"

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
