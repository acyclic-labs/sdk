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

work_parent="${TMPDIR:-/tmp}"
if command -v wslpath >/dev/null 2>&1; then
  windows_temp="$(cmd.exe /d /c echo %TEMP% | tr -d '\r')"
  work_parent="$(wslpath -u "$windows_temp")"
fi
# sccache keys Rust compilations by working directory and source paths, so a
# fresh mktemp path would miss on every run. A stable per-checkout directory,
# emptied before and after each run, keeps the package isolated and cached.
work="$work_parent/sdk-machines-package-$(printf '%s' "$root" | cksum | cut -d' ' -f1)"
rm -rf -- "$work"
(umask 077 && mkdir -- "$work")
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
  local actual flags status
  actual="$(git -C "$repository" rev-parse HEAD)" || return 1
  [[ "$actual" == "$expected" ]] || return 1
  flags="$(git -C "$repository" ls-files -v)" || return 1
  ! grep '^[a-z]' <<<"$flags" >/dev/null || return 1
  flags="$(git -C "$repository" ls-files -t)" || return 1
  ! grep '^S ' <<<"$flags" >/dev/null || return 1
  status="$(git -C "$repository" status --porcelain --untracked-files=all)" || return 1
  [[ -z "$status" ]]
}

valid_vcs_info() {
  node -e 'let input=""; process.stdin.on("data", chunk => input += chunk).on("end", () => { const expected={git:{sha1:process.argv[1]},path_in_vcs:"rust/crates/machines"}; if(JSON.stringify(JSON.parse(input)) !== JSON.stringify(expected)) process.exit(1); })' "$1"
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
bad_index="$work/bad-index"
# A directory makes Git fail; a missing index path is accepted as an empty index.
mkdir "$bad_index"
! GIT_INDEX_FILE="$bad_index" git -C "$guard_repo" ls-files -v
! GIT_INDEX_FILE="$bad_index" clean_head "$guard_repo" "$guard_head"
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

package_root="$work/package-source"
test_root="$work/test"
mkdir -p "$test_root"
git clone --quiet --no-checkout -- "$root" "$package_root"
git -C "$package_root" checkout --quiet --detach "$head"
clean_head "$package_root" "$head"

cargo_bin="cargo"
manifest="$package_root/Cargo.toml"
package_target="$work/package-target"
package_target_argument="$package_target"
if command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
  manifest="$(wslpath -w "$manifest")"
  package_target_argument="$(wslpath -w "$package_target")"
fi
versions="$("$cargo_bin" metadata --no-deps --format-version 1 --manifest-path "$manifest" | node -e 'let input=""; process.stdin.on("data", chunk => input += chunk).on("end", () => { const packages = JSON.parse(input).packages; console.log(["acyclic-machines", "acyclic-grpc-observability"].map(name => packages.find(item => item.name === name).version).join("\t")); })')"
IFS=$'\t' read -r version observability_version <<< "$versions"
"$cargo_bin" test --locked -p acyclic-machines -p acyclic-harness --manifest-path "$manifest" --target-dir "$package_target_argument"
clean_head "$root" "$head"
clean_head "$package_root" "$head"
"$cargo_bin" package --locked --no-verify -p acyclic-proto-build -p acyclic-grpc-observability -p acyclic-machines --manifest-path "$manifest" --target-dir "$package_target_argument"
crate="$package_target/package/acyclic-machines-${version}.crate"
proto_build_crate="$package_target/package/acyclic-proto-build-${version}.crate"
observability_crate="$package_target/package/acyclic-grpc-observability-$observability_version.crate"
strict_archive "$proto_build_crate"
strict_archive "$observability_crate"
strict_archive "$crate"
tar -xOf "$crate" "acyclic-machines-${version}/.cargo_vcs_info.json" |
  valid_vcs_info "$head"
clean_head "$root" "$head"
clean_head "$package_root" "$head"

tar -xzf "$crate" -C "$test_root"
tar -xzf "$proto_build_crate" -C "$test_root"
tar -xzf "$observability_crate" -C "$test_root"
mkdir -p "$test_root/.cargo"
proto_build_path="$(bash "$root/scripts/native-tool-path.sh" "$test_root/acyclic-proto-build-$version")"
observability_path="$(bash "$root/scripts/native-tool-path.sh" "$test_root/acyclic-grpc-observability-$observability_version")"
cat >"$test_root/.cargo/config.toml" <<EOF
[patch.crates-io]
acyclic-proto-build = { path = "$proto_build_path" }
acyclic-grpc-observability = { path = "$observability_path" }
EOF
install -m 0644 "$root/rust-toolchain.toml" "$test_root/rust-toolchain.toml"
test_manifest="$test_root/acyclic-machines-${version}/Cargo.toml"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  test_manifest="$(wslpath -w "$test_manifest")"
fi
(
  cd "$test_root"
  "$cargo_bin" check --manifest-path "$test_manifest" --target-dir "$package_target_argument"
)

if [[ -n "$output" ]]; then
  mkdir -p "$(dirname "$output")"
  mkdir "$output"
  install -m 0644 "$crate" "$observability_crate" "$proto_build_crate" "$output/"
  staged="$output/$(basename "$crate")"
  cmp --silent "$crate" "$staged"
  strict_archive "$staged"
  (cd "$output" && sha256sum "$(basename "$crate")" "$(basename "$observability_crate")" "$(basename "$proto_build_crate")" > SHA256SUMS && sha256sum --strict --check SHA256SUMS)
fi
