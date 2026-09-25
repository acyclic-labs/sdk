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
wasm_output="$work/generated-wasm"
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
source_sha=$(git rev-parse --verify HEAD)
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
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic-labs/harness":"file:$bun_archive_url","@bufbuild/protobuf":"2.14.1","fake-indexeddb":"6.2.4"}}
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
for entry in "${closure[@]}"; do
  IFS=$'\t' read -r name version <<< "$entry"
  if [[ "$name" == "acyclic-harness" ]]; then
    harness_version="$version"
  else
    dependency_names+=("$name")
  fi
done
[[ -n "$harness_version" ]]
for entry in "${closure[@]}"; do
  IFS=$'\t' read -r name version <<< "$entry"
  [[ "$version" == "$harness_version" ]] || { echo "Harness dependency version mismatch: $name" >&2; exit 1; }
done
package_target="$work/package-target"
cargo_package_target="$package_target"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  cargo_package_target="$(wslpath -w "$cargo_package_target")"
fi
package_arguments=()
for name in "${dependency_names[@]}" acyclic-harness; do
  package_arguments+=(-p "$name")
done
"$cargo_bin" package --locked --no-verify --allow-dirty --target-dir "$cargo_package_target" \
  "${package_arguments[@]}"
harness_crate="$package_target/package/acyclic-harness-$harness_version.crate"

mkdir "$work/crates"
for name in "${dependency_names[@]}"; do
  tar -xf "$package_target/package/$name-$harness_version.crate" -C "$work/crates"
done
tar -xf "$harness_crate" -C "$work/crates"
mkdir -p "$work/crates/.cargo"
install -m 0644 "$root/rust-toolchain.toml" "$work/crates/rust-toolchain.toml"
printf '[patch.crates-io]\n' >"$work/crates/.cargo/config.toml"
for name in "${dependency_names[@]}"; do
  patch_path="$work/crates/$name-$harness_version"
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
for name in "${dependency_names[@]}"; do
  install -m 0644 "$package_target/package/$name-$harness_version.crate" "$output/"
done
install -m 0644 "$harness_crate" "$output/"
cmp --silent "$archive" "$output/acyclic-harness.tgz"
for name in "${dependency_names[@]}"; do
  cmp --silent "$package_target/package/$name-$harness_version.crate" "$output/$name-$harness_version.crate"
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
for name in "${dependency_names[@]}"; do
  evidence_artifacts+=("$output/$name-$harness_version.crate")
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
