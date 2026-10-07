#!/usr/bin/env bash
set -euo pipefail

# Build an isolated Linux/macOS filesystem lane.  This deliberately does not
# write generated artifacts back into the checkout: the Windows lane and the
# source-attestation runner must consume their own package trees.
#
# usage:
#   native-platform-lane.sh SOURCE_ROOT OUTPUT_ROOT SOURCE_COMMIT
#
# SOURCE_ROOT must be an absolute, clean, frozen checkout. OUTPUT_ROOT must be
# an absent absolute directory on the native host. The output contains the
# staged @acyclic-labs/fs package, its platform companion archive, installed
# package tree, and the final filesystem admission receipt.

[[ $# == 3 ]] || {
  echo "usage: $0 SOURCE_ROOT OUTPUT_ROOT SOURCE_COMMIT" >&2
  exit 2
}

root="$(cd "$1" && pwd -P)"
output="$2"
source_commit="$3"

[[ "$root" == /* && "$output" == /* ]] || {
  echo "source and output paths must be absolute" >&2
  exit 2
}
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || {
  echo "source commit must be a 40-character lowercase Git revision" >&2
  exit 2
}
[[ ! -e "$output" && ! -L "$output" ]] || {
  echo "output must be absent: $output" >&2
  exit 2
}

case "$(uname -s)" in
  Linux) native_library="libacyclic_fs_napi.so"; platform="linux" ;;
  Darwin) native_library="libacyclic_fs_napi.dylib"; platform="darwin" ;;
  *) echo "this lane supports Linux and macOS only" >&2; exit 2 ;;
esac
if command -v sha256sum >/dev/null 2>&1; then
  hash_file() { sha256sum "$@"; }
elif command -v shasum >/dev/null 2>&1; then
  hash_file() { shasum -a 256 "$@"; }
else
  echo "a SHA-256 utility (sha256sum or shasum) is required" >&2
  exit 2
fi
architecture="$(uname -m)"
case "$architecture" in
  x86_64) node_arch="x64" ;;
  aarch64|arm64) node_arch="arm64" ;;
  *) echo "unsupported native architecture: $architecture" >&2; exit 2 ;;
esac

# WSL sees Windows worktree gitfiles as `Q:/...`, while Git for Linux needs
# `/mnt/q/...`. Resolve that indirection without changing the source tree so a
# native build can still attest the actual checkout rather than a copied tree.
git_dir_env=()
if [[ -f "$root/.git" ]]; then
  gitfile="$(cat "$root/.git")"
  gitdir_spec="${gitfile#gitdir: }"
  if [[ "$gitdir_spec" =~ ^([A-Za-z]):/(.*)$ ]]; then
    drive="${BASH_REMATCH[1],,}"
    git_dir_env=("GIT_DIR=/mnt/$drive/${BASH_REMATCH[2]}" "GIT_WORK_TREE=$root")
  fi
fi
git_repo() { env "${git_dir_env[@]}" git -C "$root" "$@"; }

actual_commit="$(git_repo rev-parse HEAD)"
[[ "$actual_commit" == "$source_commit" ]] || {
  echo "source HEAD $actual_commit differs from requested frozen revision $source_commit" >&2
  exit 1
}
[[ -z "$(git_repo status --porcelain=v1 --untracked-files=all)" ]] || {
  echo "source checkout is dirty; freeze and commit it before the native lane" >&2
  exit 1
}

rustc_version="$(rustc --version)"
cargo_version="$(cargo --version)"
bun_version="$(bun --version)"
node_version="$(node --version)"
grep -q 'channel = "1.98.1"' "$root/rust-toolchain.toml" || {
  echo "rust-toolchain.toml is no longer pinned to 1.98.1" >&2
  exit 1
}
expected_bun_version="${ACYCLIC_FS_BUN_VERSION:-1.4.2}"
[[ "$bun_version" == "$expected_bun_version" ]] || {
  echo "native lane requires maintained Bun $expected_bun_version (found $bun_version)" >&2
  exit 1
}

mkdir -p "$output"
target_root="$output/cargo-target"
native_stage="$output/native-stage"
package_stage="$output/package-stage"
install_root="$output/install"
wasm_stage="$output/wasm-build"
mkdir -p "$install_root" "$wasm_stage"

# Resolve binding/tool versions from the frozen workspace rather than copying
# historical values into this lane. Cargo.lock is the native authority;
# package.json/bun.lock are the maintained CLI authority.
metadata_file="$output/cargo-metadata.json"
(cd "$root" && cargo metadata --locked --format-version 1 > "$metadata_file")
resolved_versions="$(bun -e '
const metadata = JSON.parse(await Bun.stdin.text());
for (const name of ["napi", "napi-derive", "napi-build", "wasm-bindgen"]) {
  const matches = metadata.packages.filter((item) => item.name === name);
  if (matches.length !== 1) throw new Error(`expected one resolved ${name} package`);
  console.log(`${name}=${matches[0].version}`);
}
' < "$metadata_file")"
while IFS='=' read -r name version; do
  case "$name" in
    napi) napi_version="$version" ;;
    napi-derive) napi_derive_version="$version" ;;
    napi-build) napi_build_version="$version" ;;
    wasm-bindgen) wasm_bindgen_version="$version" ;;
  esac
done <<< "$resolved_versions"
wasm_bindgen_requirement="$(cd "$root" && bun -e '
const text = await Bun.file("Cargo.toml").text();
const match = text.match(/^wasm-bindgen = "=([^"]+)"/m);
if (!match) throw new Error("Cargo.toml must pin wasm-bindgen exactly");
console.log(match[1]);
')"
[[ "$wasm_bindgen_requirement" == "$wasm_bindgen_version" ]] || {
  echo "resolved wasm-bindgen $wasm_bindgen_version differs from exact manifest pin $wasm_bindgen_requirement" >&2
  exit 1
}
napi_cli_version="$(cd "$root" && bun -e '
const packageJson = JSON.parse(await Bun.file("package.json").text());
const version = packageJson.devDependencies?.["@napi-rs/cli"];
if (typeof version !== "string" || !/^\d+\.\d+\.\d+$/.test(version)) throw new Error("package.json must pin @napi-rs/cli exactly");
console.log(version);
')"
bun_lock_cli_version="$(cd "$root" && bun -e '
const lock = await Bun.file("bun.lock").text();
const match = lock.match(/"@napi-rs\/cli": \["@napi-rs\/cli@([^" ]+)"/);
if (!match) throw new Error("bun.lock lacks resolved @napi-rs/cli");
console.log(match[1]);
')"
[[ "$napi_cli_version" == "$bun_lock_cli_version" ]] || {
  echo "bun.lock @napi-rs/cli $bun_lock_cli_version differs from package.json pin $napi_cli_version" >&2
  exit 1
}

export CARGO_TARGET_DIR="$target_root"
export ACYCLIC_FS_NAPI_PROFILE=debug

# A clean Git archive has no workspace install. Bootstrap only the frozen
# lockfile dependencies before the maintained adapter and TypeScript checks;
# Bun writes the ignored node_modules tree inside this isolated checkout.
(cd "$root" && bun install --frozen-lockfile --ignore-scripts --no-progress)

(cd "$root" && cargo build --locked --manifest-path rust/crates/filesystem-napi/Cargo.toml)
native_source="$target_root/debug/$native_library"
[[ -f "$native_source" ]] || { echo "missing native library: $native_source" >&2; exit 1; }

# The maintained adapter check imports the package's compiled TypeScript
# surface. Build it before spawning the N-API child; the WASM build below is
# independent and remains isolated in its own output directory.
(cd "$root" && bun x tsc -p typescript/packages/filesystem/tsconfig.json --pretty false)

# This maintained check copies the native library into a private process and
# emits the package-shaped .node companion after ABI and adapter conformance.
(cd "$root" && bun scripts/check-filesystem-napi.mjs --adapter "$native_stage")
native_binding="$native_stage/acyclic-fs-0.2.0-${platform}-${node_arch}.node"
[[ -f "$native_binding" ]] || { echo "missing native companion: $native_binding" >&2; exit 1; }

# Build the package's WASM pair directly into the isolated package tree. The
# TypeScript build writes only ignored dist output in the checkout; the stage
# receives the exact package bytes that are subsequently archived.
(cd "$root" && bun scripts/build-wasm.mjs filesystem "$wasm_stage")
(cd "$root" && bash scripts/stage-npm-package.sh typescript/packages/filesystem "$package_stage")
# stage-npm-package copies the package-owned generated directory; replace only
# the generated WASM pair with the isolated build above.
rm -rf "$package_stage/generated/wasm"
mkdir -p "$package_stage/generated"
cp -a "$wasm_stage" "$package_stage/generated/wasm"

package_archive="$output/acyclic-fs-0.2.0.tgz"
(cd "$package_stage" && bun pm pack --ignore-scripts --filename "$package_archive" --quiet)

companion_stage="$output/companion-stage"
mkdir -p "$companion_stage"
companion_name="acyclic-fs-0.2.0-${platform}-${node_arch}.node"
cp "$native_binding" "$companion_stage/$companion_name"
cat > "$companion_stage/package.json" <<EOF
{
  "name": "@acyclic-labs/fs-${platform}-${node_arch}",
  "version": "0.2.0",
  "type": "module",
  "main": "./${companion_name}",
  "exports": { ".": "./${companion_name}" },
  "files": ["${companion_name}"],
  "os": ["${platform}"],
  "cpu": ["${node_arch}"]
}
EOF
companion_archive="$output/acyclic-labs-fs-${platform}-${node_arch}-0.2.0.tgz"
(cd "$companion_stage" && bun pm pack --ignore-scripts --filename "$companion_archive" --quiet)

# Install both archives into a private consumer. The admission runner loads the
# exact installed package root and exact installed .node path below; no
# workspace resolution is allowed to satisfy either import.
cat > "$install_root/package.json" <<EOF
{
  "private": true,
  "type": "module",
  "dependencies": {
    "@acyclic-labs/fs": "file:${package_archive}",
    "@acyclic-labs/fs-${platform}-${node_arch}": "file:${companion_archive}"
  }
}
EOF
(cd "$install_root" && bun install --ignore-scripts --no-progress)
installed_package="$install_root/node_modules/@acyclic-labs/fs"
installed_binding="$install_root/node_modules/@acyclic-labs/fs-${platform}-${node_arch}/$companion_name"
[[ -f "$installed_package/generated/wasm/acyclic_fs_wasm.js" ]] || { echo "installed package lacks WASM JS" >&2; exit 1; }
[[ -f "$installed_package/generated/wasm/acyclic_fs_wasm_bg.wasm" ]] || { echo "installed package lacks WASM binary" >&2; exit 1; }
[[ -f "$installed_binding" ]] || { echo "installed companion lacks native binding" >&2; exit 1; }

source_manifest="$output/source-attestation.json"
receipt="$output/filesystem-admission.json"
(cd "$root" && node research/qualified-prototypes/filesystem-admission/run.mjs \
  --write-source-manifest \
  --source-root "$root" \
  --source-manifest "$source_manifest")
(cd "$root" && node research/qualified-prototypes/filesystem-admission/run.mjs \
  --package-root "$installed_package" \
  --native-binding "$installed_binding" \
  --native-archive "$companion_archive" \
  --source-root "$root" \
  --source-manifest "$source_manifest" \
  --source-commit "$source_commit" \
  --archive "$package_archive" \
  --receipt "$receipt")

hash_file "$package_archive" "$companion_archive" "$installed_binding" \
  "$installed_package/generated/wasm/acyclic_fs_wasm.js" \
  "$installed_package/generated/wasm/acyclic_fs_wasm_bg.wasm" \
  > "$output/SHA256SUMS"
cat > "$output/toolchain.json" <<EOF
{
  "source_commit": "$source_commit",
  "platform": "$platform",
  "node_arch": "$node_arch",
  "rustc": "$rustc_version",
  "cargo": "$cargo_version",
  "bun": "$bun_version",
  "node": "$node_version",
  "wasm_bindgen_requirement": "$wasm_bindgen_requirement",
  "wasm_bindgen_resolved": "$wasm_bindgen_version",
  "napi_cli": "$napi_cli_version",
  "napi": "$napi_version",
  "napi_derive": "$napi_derive_version",
  "napi_build": "$napi_build_version"
}
EOF
echo "native platform lane passed: $platform-$node_arch ($source_commit)"
