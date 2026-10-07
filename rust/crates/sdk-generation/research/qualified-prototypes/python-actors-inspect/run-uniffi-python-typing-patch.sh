#!/usr/bin/env bash
set -euo pipefail

# Fetch the pinned maintained UniFFI source, apply the small reviewed patch,
# then generate Python bindings. All build and generated-output paths are
# caller supplied and remain outside this repository.
: "${SDK_ROOT:?set SDK_ROOT to the Rust SDK checkout}"
: "${OUTPUT_DIR:?set OUTPUT_DIR to an external generated-output directory}"
: "${NATIVE_LIBRARY:?set NATIVE_LIBRARY to the matching external native library}"

CARGO_BIN="${CARGO_BIN:-cargo}"
CURL_BIN="${CURL_BIN:-curl}"
SHA256_BIN="${SHA256_BIN:-sha256sum}"
TAR_BIN="${TAR_BIN:-tar}"
UPSTREAM_VERSION="${UPSTREAM_VERSION:-0.31.0}"
UPSTREAM_REV="${UPSTREAM_REV:-309762f55db3f0548194a9ceba3027fa64b18a93}"
UPSTREAM_ARCHIVE_SHA256="${UPSTREAM_ARCHIVE_SHA256:-4ED0150801958D4825DA56A41C71F000A457AC3A4613FA9647DF78AC4B6B6881}"
UPSTREAM_ARCHIVE_URL="${UPSTREAM_ARCHIVE_URL:-https://crates.io/api/v1/crates/uniffi_bindgen/${UPSTREAM_VERSION}/download}"
PATCH_FILE="${PATCH_FILE:-${SDK_ROOT}/rust/crates/sdk-generation/research/qualified-prototypes/python-actors-inspect/uniffi-python-typing.patch}"
NOMINAL_PATCH_FILE="${NOMINAL_PATCH_FILE:-${SDK_ROOT}/rust/crates/sdk-generation/research/qualified-prototypes/python-actors-inspect/uniffi-python-nominal-readonly.patch}"
GENERATOR_CONFIG="${GENERATOR_CONFIG:-${SDK_ROOT}/rust/crates/actors-uniffi/uniffi.nominal.toml}"
WORK_DIR="${WORK_DIR:-${TMPDIR:-/tmp}/uniffi-python-typing-patch}"
TARGET_DIR="${TARGET_DIR:-${TMPDIR:-/tmp}/actors-uniffi-python-typing-target}"
ARCHIVE="${WORK_DIR}/uniffi_bindgen-${UPSTREAM_VERSION}.crate"
SOURCE_DIR="${WORK_DIR}/uniffi_bindgen-${UPSTREAM_VERSION}"
CONFIG_FILE="$(mktemp)"

mkdir -p "$WORK_DIR" "$OUTPUT_DIR" "$TARGET_DIR"
PATCHED_ROOT="$(mktemp -d "${WORK_DIR}/patched.XXXXXX")/uniffi_bindgen-${UPSTREAM_VERSION}"
if [[ ! -f "$ARCHIVE" ]]; then
    "$CURL_BIN" --fail --location --retry 3 --output "$ARCHIVE" "$UPSTREAM_ARCHIVE_URL"
fi
printf '%s  %s\n' "$UPSTREAM_ARCHIVE_SHA256" "$ARCHIVE" | "$SHA256_BIN" -c -

if [[ ! -f "$SOURCE_DIR/.cargo_vcs_info.json" ]]; then
    "$TAR_BIN" -xzf "$ARCHIVE" -C "$WORK_DIR"
fi
grep -Fq '"sha1": "'"$UPSTREAM_REV"'"' "$SOURCE_DIR/.cargo_vcs_info.json"
grep -Fq 'version = "'"$UPSTREAM_VERSION"'"' "$SOURCE_DIR/Cargo.toml"
grep -Fq 'license = "MPL-2.0"' "$SOURCE_DIR/Cargo.toml"
mkdir -p "$PATCHED_ROOT"
cp -a "$SOURCE_DIR/." "$PATCHED_ROOT/"
patch --batch --forward --strip=1 --directory="$PATCHED_ROOT" < "$PATCH_FILE"
if [[ -f "$NOMINAL_PATCH_FILE" ]]; then
    patch --batch --forward --strip=1 --directory="$PATCHED_ROOT" < "$NOMINAL_PATCH_FILE"
fi

EXPECTED_TYPES="D6435ECE1A9031BD7DA82D20FE6BB6B0AB7798744F2D5A0696C644F5D19F15F1"
EXPECTED_MODULES="F956F08A562AB09733B6CB89076D2FB7DB20BE35A43E115B1510888FFA6E3986"
EXPECTED_TEMPLATE="5874EE2E4410A207217A6A1F0AB8F590F4C16C058963D3B695A6013D42085CDF"
EXPECTED_ASYNC="7866EFE6AB436733ED83AAE51C1D06CADAF21077F48BF35C4E8E86F32D0F7F74"
EXPECTED_CALLABLE="49CC9989F34624991A95A8DD3C49D515D08ECDCD46459C566A735657B910E7CF"
EXPECTED_NODES="3A3C4B2E1CB049C48EAA08E7C71C9778C549A2812C896A0CDDC14ABADC8DC707"
EXPECTED_CUSTOM="0E033A690B837E2117583A4075ADF32C2557DAEA8D60D97640EC73403E0A99F2"
EXPECTED_RECORD="60B6CE85B3BB75A39BE4810EF38A4B1C9EE5FBF00BA9535CF24361CBA582987B"
printf '%s  %s\n' "$EXPECTED_TYPES" "$PATCHED_ROOT/src/bindings/python/pipeline/types.rs" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_MODULES" "$PATCHED_ROOT/src/bindings/python/pipeline/modules.rs" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_NODES" "$PATCHED_ROOT/src/bindings/python/pipeline/nodes.rs" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_TEMPLATE" "$PATCHED_ROOT/src/bindings/python/templates/EnumTemplate.py" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_CUSTOM" "$PATCHED_ROOT/src/bindings/python/templates/CustomType.py" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_RECORD" "$PATCHED_ROOT/src/bindings/python/templates/RecordTemplate.py" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_ASYNC" "$PATCHED_ROOT/src/bindings/python/templates/Async.py" | "$SHA256_BIN" -c -
printf '%s  %s\n' "$EXPECTED_CALLABLE" "$PATCHED_ROOT/src/bindings/python/templates/CallableBody.py" | "$SHA256_BIN" -c -

cleanup() { rm -f "$CONFIG_FILE"; }
trap cleanup EXIT
GENERATOR_CONFIG_ARGS=()
if [[ -f "$GENERATOR_CONFIG" ]]; then
  GENERATOR_CONFIG_ARGS=(-c "$GENERATOR_CONFIG")
fi
cat >"$CONFIG_FILE" <<EOF
[patch.crates-io]
uniffi_bindgen = { path = "$PATCHED_ROOT" }
EOF

RUSTC_WRAPPER= CARGO_TARGET_DIR="$TARGET_DIR" "$CARGO_BIN" run \
  --manifest-path "$SDK_ROOT/rust/crates/actors-uniffi/Cargo.toml" \
  --config "$CONFIG_FILE" \
  --features bindgen --bin actors-uniffi-bindgen -- \
  generate --library -l python --out-dir "$OUTPUT_DIR" \
  "${GENERATOR_CONFIG_ARGS[@]}" "$NATIVE_LIBRARY"




