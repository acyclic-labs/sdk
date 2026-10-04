#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && "$1" == /* ]] || { echo 'usage: check-harness-package.sh ABSOLUTE_OUTPUT' >&2; exit 2; }
output="$1"
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_platform="$(bun -e 'process.stdout.write(process.platform)')"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  windows_temp="$(cmd.exe /d /c echo %TEMP% | tr -d '\r')"
  work_parent="$(wslpath -u "$windows_temp")"
  work="$(mktemp -d "$work_parent/sdk-harness-package.XXXXXXXX")"
else
  work="$(mktemp -d -t sdk-harness-package.XXXXXXXX)"
fi
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
archive="$work/acyclic-harness.tgz"
# TypeScript resolves the Rust-generated declarations from the package tree.
# Build there once, then stage those exact bytes into the archive below.
wasm_output="$root/typescript/packages/harness/generated/wasm"
bun_archive="$archive"
bun_archive_url="$archive"
bun_wasm_output="$wasm_output"
if [[ "$bun_platform" == "win32" ]] && command -v cygpath >/dev/null 2>&1; then
  bun_archive="$(cygpath -w "$archive")"
  bun_archive_url="$(cygpath -m "$archive")"
  bun_wasm_output="$(cygpath -w "$wasm_output")"
elif [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1; then
  bun_archive="$(wslpath -w "$archive")"
  bun_archive_url="$(wslpath -m "$archive")"
  bun_wasm_output="$(wslpath -w "$wasm_output")"
fi

cd "$root"
git_bin="git"
if [[ "$bun_platform" == "win32" ]] && command -v git.exe >/dev/null 2>&1; then
  git_bin="git.exe"
fi
source_sha=$("$git_bin" rev-parse --verify HEAD)
cargo_bin="cargo"
if [[ "$bun_platform" == "win32" ]] && command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
fi
bun_wasm_bindgen_bin="$(bash "$root/scripts/ensure-wasm-bindgen.sh")"
bun scripts/check-metadata.mjs
mkdir -p "$wasm_output"
bun scripts/build-harness-wasm.mjs "$bun_wasm_output" "$cargo_bin" "$bun_wasm_bindgen_bin"
bun x tsc -p typescript/packages/harness/tsconfig.json
for generated in acyclic_harness_wasm.js acyclic_harness_wasm.d.ts \
  acyclic_harness_wasm_bg.wasm acyclic_harness_wasm_bg.wasm.d.ts; do
  [[ -s "$wasm_output/$generated" ]] || { echo "missing generated Harness artifact: $generated" >&2; exit 1; }
done
npm_stage="$work/npm-package"
mkdir -p "$npm_stage/generated"
install -m 0644 typescript/packages/harness/package.json typescript/packages/harness/README.md typescript/packages/harness/CHANGELOG.md "$npm_stage/"
cp -R typescript/packages/harness/dist "$npm_stage/dist"
cp -R typescript/packages/harness/generated/proto "$npm_stage/generated/proto"
cp -R "$wasm_output" "$npm_stage/generated/wasm"
cd "$npm_stage"
bun pm pack --ignore-scripts --filename "$bun_archive" --quiet

mkdir "$work/consumer"
protobuf_version="$(bun -e 'console.log(require(process.argv[1]).dependencies["@bufbuild/protobuf"])' "$root/typescript/packages/harness/package.json")"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic-labs/harness":"file:$bun_archive_url","@bufbuild/protobuf":"$protobuf_version","fake-indexeddb":"6.2.4"}}
EOF
cd "$work/consumer"
bun install --ignore-scripts
mkdir -p test
install -m 0644 "$root/scripts/fixtures/installed-harness/test/"*.test.ts test/
# The workspace runs the full source suite separately. Reuse the client and
# transport conformance cases here against the installed public exports.
for source_test in client wire-transport; do
  sed \
    -e 's#../src/index.js#@acyclic-labs/harness#g' \
    -e 's#../generated/proto/harness/v2/harness_pb.js#@acyclic-labs/harness/proto#g' \
    -e 's#../generated/proto/protocol/v1/protocol_pb.js#@acyclic-labs/harness/protocol#g' \
    "$root/typescript/packages/harness/test/$source_test.test.ts" >"test/$source_test.test.ts"
  ! grep -Eq '\.\./(src|generated)/' "test/$source_test.test.ts" || {
    echo "installed consumer test still imports Harness internals: $source_test" >&2
    exit 1
  }
done
install -m 0644 "$root/conformance/vectors/harness/native-wasm-event-v2.json" native-wasm-event-v2.json
bun test test 2>&1 | tee "$work/typescript-package-test.log"

cd "$root"
# Stage the public Harness dependency closure from the same source as receipt
# verification, including optional adapters.
closure_output="$(bun scripts/harness-package-closure.mjs)"
mapfile -t closure <<< "$closure_output"
harness_version=""
dependency_names=()
dependency_versions=()
for entry in "${closure[@]}"; do
  IFS=$'\t' read -r name version <<< "$entry"
  if [[ "$name" == "acyclic-harness" ]]; then
    harness_version="$version"
  else
    dependency_names+=("$name")
    dependency_versions+=("$version")
  fi
done
[[ -n "$harness_version" ]]
package_target="$work/package-target"
cargo_package_target="$package_target"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  cargo_package_target="$(wslpath -w "$cargo_package_target")"
fi
package_arguments=()
for name in "${dependency_names[@]}" acyclic-harness; do
  package_arguments+=(-p "$name")
done
package_config=()
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  case "$name" in
    acyclic-sdk-contract-options|acyclic-sdk-contract-wire)
      contract_source="$root/rust/crates/${name#acyclic-}"
      if [[ "$cargo_bin" == "cargo.exe" ]]; then
        contract_source="$(bash "$root/scripts/native-tool-path.sh" "$contract_source")"
      fi
      package_config+=(--config "patch.crates-io.$name.path=\"$contract_source\"")
      ;;
  esac
done
"$cargo_bin" package --locked --no-verify --allow-dirty --target-dir "$cargo_package_target" \
  "${package_config[@]}" \
  "${package_arguments[@]}"
harness_crate="$package_target/package/acyclic-harness-$harness_version.crate"

mkdir "$work/crates"
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  version="${dependency_versions[$index]}"
  tar -xf "$package_target/package/$name-$version.crate" -C "$work/crates"
done
tar -xf "$harness_crate" -C "$work/crates"
# The wire build crate records the canonical family model inputs with
# source-relative include_bytes! paths. Preserve those exact Rust-owned bytes
# in the extracted consumer layout so the packaged build uses the same inputs
# as the producer tree, including the private contract source snapshots.
wire_version=""
for index in "${!dependency_names[@]}"; do
  if [[ "${dependency_names[$index]}" == "acyclic-sdk-contract-wire" ]]; then
    wire_version="${dependency_versions[$index]}"
  fi
done
if [[ -n "$wire_version" ]]; then
  stage_wire_input() {
    source="$root/$1"
    destination="$work/crates/$2"
    [[ -f "$source" ]] || { echo "missing canonical wire input: $source" >&2; exit 1; }
    mkdir -p "$(dirname "$destination")"
    install -m 0644 "$source" "$destination"
  }
  stage_wire_input rust/crates/actors/src/generated/acyclic-actors-v1.bin actors/src/generated/acyclic-actors-v1.bin
  stage_wire_input rust/crates/workers/src/generated/acyclic-workers-v1.bin workers/src/generated/acyclic-workers-v1.bin
  stage_wire_input rust/crates/objects/src/generated/acyclic-objects-v2.bin objects/src/generated/acyclic-objects-v2.bin
  stage_wire_input rust/crates/stream/proto/stream/v2/stream_descriptor.bin stream/proto/stream/v2/stream_descriptor.bin
  stage_wire_input rust/crates/inference/inference_descriptor.bin inference/inference_descriptor.bin
  stage_wire_input rust/crates/machines/src/generated/acyclic-machines-v1.bin machines/src/generated/acyclic-machines-v1.bin
  stage_wire_input rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin filesystem/src/generated/acyclic-filesystem-v2.bin
  stage_wire_input rust/crates/harness/src/generated/harness-archived-v2.bin harness/src/generated/harness-archived-v2.bin
  stage_wire_input rust/crates/sdk-contract-options/src/lib.rs sdk-contract-options/src/lib.rs
  stage_wire_input rust/crates/sdk-contract-options/Cargo.toml sdk-contract-options/Cargo.toml
  stage_wire_input rust/crates/sdk-contract-options/Cargo.lock sdk-contract-options/Cargo.lock
  stage_wire_input rust/crates/sdk-contract-validation/src/lib.rs sdk-contract-validation/src/lib.rs
  stage_wire_input rust/crates/sdk-contract-validation/Cargo.toml sdk-contract-validation/Cargo.toml
  stage_wire_input rust/crates/sdk-contract-validation/Cargo.lock sdk-contract-validation/Cargo.lock
fi
mkdir -p "$work/crates/.cargo"
install -m 0644 "$root/rust-toolchain.toml" "$work/crates/rust-toolchain.toml"
printf '[patch.crates-io]\n' >"$work/crates/.cargo/config.toml"
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  version="${dependency_versions[$index]}"
  patch_path="$work/crates/$name-$version"
  if [[ "$bun_platform" == "win32" ]]; then
    patch_path="$(bash "$root/scripts/native-tool-path.sh" "$patch_path")"
  fi
  printf '%s = { path = "%s" }\n' "$name" "$patch_path" >>"$work/crates/.cargo/config.toml"
done
cd "$work/crates"
"$cargo_bin" test --manifest-path "acyclic-harness-$harness_version/Cargo.toml" --all-features --offline \
  -- --test-threads=1 2>&1 | tee "$work/rust-package-test.log"

mkdir -p "$output"
install -m 0644 "$archive" "$output/"
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  version="${dependency_versions[$index]}"
  install -m 0644 "$package_target/package/$name-$version.crate" "$output/"
done
install -m 0644 "$harness_crate" "$output/"
cmp --silent "$archive" "$output/acyclic-harness.tgz"
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  version="${dependency_versions[$index]}"
  cmp --silent "$package_target/package/$name-$version.crate" "$output/$name-$version.crate"
done
cmp --silent "$harness_crate" "$output/acyclic-harness-$harness_version.crate"
normalizer="$root/scripts/normalize-harness-evidence.mjs"
rust_log="$work/rust-package-test.log"
typescript_log="$work/typescript-package-test.log"
evidence_output="$output/CONFORMANCE-EVIDENCE.json"
evidence_artifacts=(
  "$output/acyclic-harness.tgz"
  "$output/acyclic-harness-$harness_version.crate"
)
for index in "${!dependency_names[@]}"; do
  name="${dependency_names[$index]}"
  version="${dependency_versions[$index]}"
  evidence_artifacts+=("$output/$name-$version.crate")
done
if [[ "$bun_platform" == "win32" ]]; then
  normalizer="$(bash "$root/scripts/native-tool-path.sh" "$normalizer")"
  rust_log="$(bash "$root/scripts/native-tool-path.sh" "$rust_log")"
  typescript_log="$(bash "$root/scripts/native-tool-path.sh" "$typescript_log")"
  evidence_output="$(bash "$root/scripts/native-tool-path.sh" "$evidence_output")"
  for index in "${!evidence_artifacts[@]}"; do
    evidence_artifacts[$index]="$(bash "$root/scripts/native-tool-path.sh" "${evidence_artifacts[$index]}")"
  done
fi
bun "$normalizer" "$rust_log" "$typescript_log" "$evidence_output" "${evidence_artifacts[@]}"
repeat_evidence="$work/CONFORMANCE-EVIDENCE.repeat.json"
repeat_evidence_arg="$repeat_evidence"
[[ "$bun_platform" != "win32" ]] || repeat_evidence_arg="$(bash "$root/scripts/native-tool-path.sh" "$repeat_evidence")"
bun "$normalizer" "$rust_log" "$typescript_log" "$repeat_evidence_arg" "${evidence_artifacts[@]}"
cmp --silent "$output/CONFORMANCE-EVIDENCE.json" "$repeat_evidence"
cd "$output"
sha256sum acyclic-harness.tgz acyclic-*.crate CONFORMANCE-EVIDENCE.json > SHA256SUMS
printf '%s\n' "$source_sha" > SOURCE_COMMIT
