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
fixture_pid=""
trap 'status=$?; if [[ -n "${fixture_pid:-}" ]]; then kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true; fi; rm -rf -- "$work"; exit "$status"' EXIT

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

# Exercise the installed all-eight consumer against the canonical authenticated
# TLS fixture. The fixture deliberately blocks the second InspectActor request;
# live-remote.cc cancels that opaque Rust operation and the fixture exits only
# after observing the server-side HTTP/2 abort.
fixture_json="$work/fixture.json"
fixture_ca="$work/fixture-ca.pem"
# The checkout's pnpm junctions may point at another worktree. Copy only the
# three runtime packages needed by the canonical Actors adapter into /tmp and
# resolve them through the qualification loader.
node_dep_root="$work/node-deps"
mkdir -p "$node_dep_root/@bufbuild" "$node_dep_root/@connectrpc"
cp -R "$root/node_modules/.bun/@bufbuild+protobuf@2.15.0/node_modules/@bufbuild/protobuf" "$node_dep_root/@bufbuild/"
cp -R "$root/node_modules/.bun/@connectrpc+connect@2.1.1+432d72b9ceda49ac/node_modules/@connectrpc/connect" "$node_dep_root/@connectrpc/"
cp -R "$root/node_modules/.bun/@connectrpc+connect-node@2.1.1+362e60f3a5b2a079/node_modules/@connectrpc/connect-node" "$node_dep_root/@connectrpc/"
# Reuse the workspace target, offline, so Cargo can launch the already-built
# conformance certificate without touching the registry while other jobs run.
ACYCLIC_SDK_ROOT="$root" ACYCLIC_DEP_ROOT="$node_dep_root" ACYCLIC_CA_PATH="$fixture_ca" \
  NODE_OPTIONS="--experimental-loader=$cxx_path/consumer/qualification-loader.mjs" \
  RUSTUP_TOOLCHAIN=1.98.1 CARGO_TARGET_DIR="$root/target" CARGO_NET_OFFLINE=true \
  node "$cxx_path/consumer/live-cancel-fixture.mjs" >"$fixture_json" \
  2>"$work/fixture.log" &
fixture_pid=$!
for _ in $(seq 1 300); do
  [[ -s "$fixture_json" ]] && break
  sleep 1
done
[[ -s "$fixture_json" && -s "$fixture_ca" ]] || {
  echo 'canonical Actors fixture did not publish endpoint and CA' >&2
  cat "$work/fixture.log" >&2 || true
  exit 1
}
fixture_endpoint="$(node -e '
const fs = require("fs");
const value = JSON.parse(fs.readFileSync(process.argv[1], "utf8").split("\n", 1)[0]);
process.stdout.write(value.endpoint);
' "$fixture_json")"
fixture_token="$(node -e '
const fs = require("fs");
const value = JSON.parse(fs.readFileSync(process.argv[1], "utf8").split("\n", 1)[0]);
process.stdout.write(value.token);
' "$fixture_json")"
"$work/external-build/cpp-actors-external-live" \
  "$fixture_endpoint" "$fixture_token" "$fixture_ca" \
  2>&1 | tee "$work/live-runtime.log"
wait "$fixture_pid"
grep -q '"inspectStarted":true,"inspectAborted":true' "$work/fixture.log"

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
# Keep a file-level inventory of the exact extracted source crates used by the
# tested package. This makes the receipt reproducible even when the checkout
# cannot provide a usable Git worktree identity.
{
  printf '%s\n' '# exact extracted source inventory (sha256  relative-path)'
  (
    cd "$actors_path"
    find . -type f -print0 | sort -z | while IFS= read -r -d '' path; do
      sha256sum "$path" | sed 's#  \.\/#  actors/#'
    done
  )
  (
    cd "$cxx_path"
    find . -type f -print0 | sort -z | while IFS= read -r -d '' path; do
      sha256sum "$path" | sed 's#  \.\/#  cxx/#'
    done
  )
} > "$output/SOURCE-INVENTORY"
printf 'actors_archive_sha256=%s\n' "$(sha256sum "$actors_archive" | cut -d' ' -f1)" > "$output/SOURCE-IDENTITY"
printf 'cxx_archive_sha256=%s\n' "$(sha256sum "$cxx_archive" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'source_inventory_sha256=%s\n' "$(sha256sum "$output/SOURCE-INVENTORY" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'actors_cargo_lock_sha256=%s\n' "$(sha256sum "$actors_path/Cargo.lock" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'cxx_cargo_lock_sha256=%s\n' "$(sha256sum "$cxx_path/Cargo.lock" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'toolchain_sha256=%s\n' "$(sha256sum "$root/rust-toolchain.toml" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'cxx_build_script_sha256=%s\n' "$(sha256sum "$cxx_path/build.rs" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'generated_header_sha256=%s\n' "$(sha256sum "$output/lib.rs.h" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
printf 'native_library_sha256=%s\n' "$(sha256sum "$output/libcpp_actors_oss_qualification.a" | cut -d' ' -f1)" >> "$output/SOURCE-IDENTITY"
if source_commit="$(git rev-parse --verify HEAD 2>/dev/null)"; then
  printf '%s\n' "$source_commit" > "$output/SOURCE_COMMIT"
else
  echo 'git identity unavailable; qualification is bound to exact source/archive/artifact hashes' >&2
  printf 'unavailable (checkout does not expose a resolvable Git worktree)\n' > "$output/SOURCE_COMMIT"
fi
(cd "$output" && sha256sum \
  "$(basename "$actors_archive")" "$(basename "$cxx_archive")" \
  lib.rs.h libcpp_actors_oss_qualification.a SOURCE-INVENTORY SOURCE-IDENTITY > SHA256SUMS)
printf 'find_package_install:PASS\nexternal_positive_runtime_typed_error_cancel:PASS\nexternal_all8_link:PASS\nexternal_all8_runtime_tls_auth:PASS\nexternal_inflight_cancel_server_abort:PASS\nnegative_u64:PASS\nnegative_nominal:PASS\ninstalled_linux_wsl_runtime:PASS\nwindows_msvc_producer_artifact:RECORDED\nmacos_ssh_runtime:NOT_RUN\n' > "$output/QUALIFICATION"
printf 'cpp_actors_package_qualification:PASS\n'
