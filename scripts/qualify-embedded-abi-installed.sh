#!/usr/bin/env bash
set -euo pipefail

# Qualify C, C++, and Python consumers against the already aggregated native
# package. This script deliberately never invokes Cargo: each consumer uses
# the exact runtime and ABI header delivered by aggregate-and-package.
ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PACKAGE_ROOT="${PACKAGE_ROOT:?PACKAGE_ROOT must point at embedded-native-merged}"
RID="${RID:?RID is required}"
BUILD_DIRECTORY="${BUILD_DIRECTORY:-${RUNNER_TEMP:-$ROOT}/acyclic-embedded-abi-installed-$RID}"

case "$BUILD_DIRECTORY" in
  "$ROOT"|"$ROOT"/*)
    echo "BuildDirectory must be outside the source checkout: $BUILD_DIRECTORY" >&2
    exit 2
    ;;
esac

case "$RID" in
  linux-x64|linux-arm64|linux-musl-x64|linux-musl-arm64)
    RUNTIME_NAME="libacyclic_sdk_embedded_prototype.so"
    RUNTIME_ENV="LD_LIBRARY_PATH"
    ;;
  osx-x64|osx-arm64)
    RUNTIME_NAME="libacyclic_sdk_embedded_prototype.dylib"
    RUNTIME_ENV="DYLD_LIBRARY_PATH"
    ;;
  *) echo "unsupported Unix embedded RID: $RID" >&2; exit 2 ;;
esac

PACKAGE_ROOT="$(cd "$PACKAGE_ROOT" && pwd)"
BUILD_DIRECTORY="$(mkdir -p "$BUILD_DIRECTORY" && cd "$BUILD_DIRECTORY" && pwd)"
RID_ROOT="$PACKAGE_ROOT/$RID"
RUNTIME="$RID_ROOT/$RUNTIME_NAME"
HEADER="$PACKAGE_ROOT/abi/acyclic_embedded_prototype.h"
CONSUMER_SOURCE="$ROOT/cpp/embedded-consumer"
CONSUMER_BUILD="$BUILD_DIRECTORY/consumer"
PREFIX="$BUILD_DIRECTORY/prefix"
INSTALLED_BUILD="$BUILD_DIRECTORY/installed-consumer"

for required in "$RUNTIME" "$HEADER"; do
  [[ -f "$required" ]] || { echo "Installed ABI input is missing: $required" >&2; exit 2; }
done
command -v cmake >/dev/null
command -v ctest >/dev/null
command -v ninja >/dev/null
command -v c++ >/dev/null
command -v cc >/dev/null
command -v python3 >/dev/null

CMAKE_PLATFORM_ARGS=()
C_CONSUMER_PLATFORM_ARGS=()
PYTHON_RUNNER=(python3)
if [[ "$(uname -s)" == Darwin && "${CMAKE_OSX_ARCHITECTURES:-}" == x86_64 ]]; then
  CMAKE_PLATFORM_ARGS+=("-DCMAKE_OSX_ARCHITECTURES=x86_64")
  C_CONSUMER_PLATFORM_ARGS+=("-arch" x86_64)
  if [[ "$(uname -m)" == arm64 ]]; then
    command -v arch >/dev/null
    PYTHON_RUNNER=(arch -x86_64 python3)
  fi
fi

assert_musl_executable() {
  [[ "$RID" == linux-musl-* ]] || return 0
  command -v readelf >/dev/null
  readelf -l "$1" | grep -Eq 'Requesting program interpreter: .*/ld-musl([^/]*)?\.so\.1'
}

cmake -S "$CONSUMER_SOURCE" -B "$CONSUMER_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="${CXX:-c++}" \
  -DACYCLIC_EMBEDDED_ROOT="$RID_ROOT" \
  -DACYCLIC_EMBEDDED_HEADER="$HEADER" \
  "${CMAKE_PLATFORM_ARGS[@]}"
cmake --build "$CONSUMER_BUILD"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_consumer"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_negative"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_cross_thread"
ctest --test-dir "$CONSUMER_BUILD" --output-on-failure
cmake --install "$CONSUMER_BUILD" --prefix "$PREFIX"

INSTALLED_RUNTIME="$(find "$PREFIX" -type f -name "$RUNTIME_NAME" -print -quit)"
[[ -n "$INSTALLED_RUNTIME" && -f "$PREFIX/include/acyclic_embedded_prototype.h" ]]
INSTALLED_LIBRARY_DIRECTORY="$(dirname "$INSTALLED_RUNTIME")"

cmake -S "$CONSUMER_SOURCE/install-consumer" -B "$INSTALLED_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="${CXX:-c++}" -DCMAKE_PREFIX_PATH="$PREFIX" \
  "${CMAKE_PLATFORM_ARGS[@]}"
cmake --build "$INSTALLED_BUILD"
assert_musl_executable "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"

C_CONSUMER="$BUILD_DIRECTORY/c-consumer"
"${CC:-cc}" "${C_CONSUMER_PLATFORM_ARGS[@]}" -std=c11 -I"$PREFIX/include" \
  "$ROOT/rust/crates/sdk-embedded-prototype/tests/c_consumer.c" \
  "$INSTALLED_RUNTIME" -o "$C_CONSUMER"
assert_musl_executable "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" \
  "${PYTHON_RUNNER[@]}" "$ROOT/rust/crates/sdk-embedded-prototype/tests/python_consumer.py" "$INSTALLED_RUNTIME"

"${PYTHON_RUNNER[@]}" - "$ROOT" "$PACKAGE_ROOT" "$RID" "$RUNTIME_NAME" "$INSTALLED_RUNTIME" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

root, package, rid, runtime_name, runtime_path = map(pathlib.Path, sys.argv[1:])
source_revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
runtime_hash = hashlib.sha256(runtime_path.read_bytes()).hexdigest()
header_hash = hashlib.sha256((package / "abi/acyclic_embedded_prototype.h").read_bytes()).hexdigest()
receipt = {
    "schema": "acyclic.sdk.embedded.abi-installed.v1",
    "status": "passed",
    "rid": rid.name,
    "source_revision": source_revision,
    "package_root": "embedded-native-merged",
    "runtime": f"{rid.name}/{runtime_name}",
    "runtime_sha256": runtime_hash,
    "header": "abi/acyclic_embedded_prototype.h",
    "header_sha256": header_hash,
    "consumers": ["installed C++ CMake consumer", "installed C consumer", "installed Python consumer"],
}
out = pathlib.Path.cwd() / "abi-installed-receipt.json"
out.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
PY

echo "Installed embedded ABI consumers passed for $RID"
