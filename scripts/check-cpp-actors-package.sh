#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || {
  echo 'usage: check-cpp-actors-package.sh ABSOLUTE_OUTPUT' >&2
  exit 2
}
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || {
  echo 'package output must be absent' >&2
  exit 2
}
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
prototype="$root/rust/crates/sdk-generation/research/qualified-prototypes/cpp-actors-oss"
work="$(mktemp -d -t sdk-cpp-actors-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT

cd "$root"
metadata="$(cargo metadata --locked --no-deps --format-version 1)"
actors_version="$(printf '%s' "$metadata" | node -e '
let input=""; process.stdin.on("data", c => input += c).on("end", () => {
  const m = JSON.parse(input);
  const p = m.packages.find(p => p.name === "acyclic-actors");
  if (!p) throw new Error("acyclic-actors package metadata missing");
  process.stdout.write(p.version);
});')"
cxx_metadata="$(cargo metadata --locked --no-deps --format-version 1 \
  --manifest-path "$prototype/Cargo.toml")"
cxx_version="$(printf '%s' "$cxx_metadata" | node -e '
let input=""; process.stdin.on("data", c => input += c).on("end", () => {
  const m = JSON.parse(input);
  const p = m.packages.find(p => p.name === "cpp-actors-oss-qualification");
  if (!p) throw new Error("cpp-actors-oss-qualification metadata missing");
  process.stdout.write(p.version);
});')"

package_target="$work/package-target"
# Actors is the authoritative Rust package. This archive is the explicit
# external dependency used below; it is never published by this qualification.
cargo package --locked --allow-dirty --no-verify --target-dir "$package_target" \
  -p acyclic-actors
actors_archive="$package_target/package/acyclic-actors-$actors_version.crate"
[[ -f "$actors_archive" ]]
mkdir -p "$work/crates"
tar -xf "$actors_archive" -C "$work/crates"
actors_path="$work/crates/acyclic-actors-$actors_version"

# Cargo normalizes the path dependency in the CXX archive to crates.io. The
# local patch only supplies the unpublished Actors archive while packaging and
# testing; it never changes the published source or hides the dependency.
cat >"$work/package-config.toml" <<EOF
[patch.crates-io]
acyclic-actors = { path = "$actors_path" }
EOF
cargo package --locked --allow-dirty --no-verify --target-dir "$package_target" \
  --manifest-path "$prototype/Cargo.toml" --config "$work/package-config.toml"
cxx_archive="$package_target/package/cpp-actors-oss-qualification-$cxx_version.crate"
[[ -f "$cxx_archive" ]]
tar -xf "$cxx_archive" -C "$work/crates"
cxx_path="$work/crates/cpp-actors-oss-qualification-$cxx_version"

mkdir -p "$work/crates/.cargo"
cp "$work/package-config.toml" "$work/crates/.cargo/config.toml"
cargo build --locked --offline --manifest-path "$cxx_path/Cargo.toml" \
  --target-dir "$work/cargo-target" \
  --config "$work/crates/.cargo/config.toml"
native_library="$work/cargo-target/debug/libcpp_actors_oss_qualification.a"
[[ -f "$native_library" ]]

install_root="$work/install"
cmake -S "$cxx_path" -B "$work/cmake-build" \
  -DCPP_ACTORS_NATIVE_LIBRARY="$native_library" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$install_root"
cmake --install "$work/cmake-build"
[[ -f "$install_root/include/lib.rs.h" ]]
[[ -f "$install_root/include/rust/cxx.h" ]]
[[ -f "$install_root/lib/cmake/AcyclicActorsCXX/AcyclicActorsCXXConfig.cmake" ]]

# Configure and build from the extracted consumer source, proving the config is
# relocatable and that the full eight-operation entrypoint surface is linkable.
cmake -S "$cxx_path/consumer/cmake" -B "$work/external-build" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$install_root"
cmake --build "$work/external-build" --config Release
"$work/external-build/cpp-actors-external-positive"

cxx_compiler="${CXX:-c++}"
if "$cxx_compiler" -std=c++11 -I"$install_root/include" -fsyntax-only \
  "$cxx_path/consumer/negative.cc" >"$work/negative.log" 2>&1; then
  echo 'negative.cc unexpectedly compiled' >&2
  exit 1
fi
if "$cxx_compiler" -std=c++11 -I"$install_root/include" -fsyntax-only \
  "$cxx_path/consumer/negative-nominal.cc" >"$work/negative-nominal.log" 2>&1; then
  echo 'negative-nominal.cc unexpectedly compiled' >&2
  exit 1
fi

mkdir -p "$output"
install -m 0644 "$actors_archive" "$cxx_archive" "$output/"
# Preserve the exact generated bridge header and Cargo-produced native artifact
# used by the external install check alongside both source archives. These are
# the bytes whose hashes identify the installed producer/consumer boundary.
install -m 0644 "$install_root/include/lib.rs.h" "$output/lib.rs.h"
install -m 0644 "$native_library" "$output/libcpp_actors_oss_qualification.a"
(cd "$output" && sha256sum \
  "$(basename "$actors_archive")" "$(basename "$cxx_archive")" \
  lib.rs.h libcpp_actors_oss_qualification.a > SHA256SUMS)
printf 'actors_archive_sha256=%s\n' "$(sha256sum "$actors_archive" | cut -d' ' -f1)" > "$output/SOURCE-IDENTITY"
printf 'cxx_archive_sha256=%s\n' "$(sha256sum "$cxx_archive" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'generated_header_sha256=%s\n' "$(sha256sum "$output/lib.rs.h" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'native_library_sha256=%s\n' "$(sha256sum "$output/libcpp_actors_oss_qualification.a" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
if source_commit="$(git rev-parse --verify HEAD 2>/dev/null)"; then
  printf '%s\n' "$source_commit" > "$output/SOURCE_COMMIT"
else
  printf 'unavailable (checkout does not expose a resolvable Git worktree)\n' > "$output/SOURCE_COMMIT"
fi
printf 'find_package:PASS\nexternal_positive:PASS\nexternal_all8_link:PASS\nnegative_u64:PASS\nnegative_nominal:PASS\n' > "$output/QUALIFICATION"
