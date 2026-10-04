#!/usr/bin/env bash
set -euo pipefail

# Cross-platform release recipe for the Rust-owned embedded ABI. The Windows
# lane uses portable-package-test.ps1; Unix and Darwin lanes use this script
# with their native runner toolchain. No platform feature flag is passed to
# Cargo: the target and linker are supplied by the release runner.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIRECTORY="${BUILD_DIRECTORY:-${RUNNER_TEMP:-$ROOT}/acyclic-embedded-${RUST_TARGET:-host}}"
RUST_TARGET="${RUST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
CXX="${CXX:-c++}"

case "$RUST_TARGET" in
  *-apple-darwin) RUNTIME_NAME="libacyclic_sdk_embedded_prototype.dylib"; RUNTIME_ENV="DYLD_LIBRARY_PATH" ;;
  *-linux-*) RUNTIME_NAME="libacyclic_sdk_embedded_prototype.so"; RUNTIME_ENV="LD_LIBRARY_PATH" ;;
  *) echo "unsupported Unix embedded target: $RUST_TARGET" >&2; exit 2 ;;
esac

MANIFEST="$ROOT/rust/crates/sdk-embedded-prototype/Cargo.toml"
TARGET_DIRECTORY="$BUILD_DIRECTORY/rust-target"
RELEASE_DIRECTORY="$TARGET_DIRECTORY/$RUST_TARGET/release"
CONSUMER_SOURCE="$ROOT/cpp/embedded-consumer"
CONSUMER_BUILD="$BUILD_DIRECTORY/consumer"
PREFIX="$BUILD_DIRECTORY/prefix"
INSTALLED_BUILD="$BUILD_DIRECTORY/installed-consumer"

mkdir -p "$BUILD_DIRECTORY"
command -v cargo >/dev/null
command -v cmake >/dev/null
command -v ctest >/dev/null
command -v "$CXX" >/dev/null

cargo build --locked --offline --release --target "$RUST_TARGET" \
  --target-dir "$TARGET_DIRECTORY" --manifest-path "$MANIFEST"

RUNTIME="$RELEASE_DIRECTORY/$RUNTIME_NAME"
HEADER="$(find "$RELEASE_DIRECTORY/build" -type f -name acyclic_embedded_prototype.h -print -quit)"
test -f "$RUNTIME"
test -n "$HEADER"

cmake -S "$CONSUMER_SOURCE" -B "$CONSUMER_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DACYCLIC_EMBEDDED_ROOT="$RELEASE_DIRECTORY" \
  -DACYCLIC_EMBEDDED_HEADER="$HEADER"
cmake --build "$CONSUMER_BUILD"
ctest --test-dir "$CONSUMER_BUILD" --output-on-failure
cmake --install "$CONSUMER_BUILD" --prefix "$PREFIX"

cmake -S "$CONSUMER_SOURCE/install-consumer" -B "$INSTALLED_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="$CXX" -DCMAKE_PREFIX_PATH="$PREFIX"
cmake --build "$INSTALLED_BUILD"
env "$RUNTIME_ENV=$PREFIX/lib" "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"

python3 - "$BUILD_DIRECTORY/platform-package.json" "$PREFIX" "$RUST_TARGET" "$RUNTIME_NAME" <<'PY'
import hashlib
import json
import pathlib
import sys

output, prefix, target, runtime = sys.argv[1:]
root = pathlib.Path(prefix)
files = {}
for path in sorted(p for p in root.rglob('*') if p.is_file()):
    files[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
json.dump({
    "schema": "acyclic.sdk.embedded.platform-package.v1",
    "status": "passed",
    "target": target,
    "runtime": runtime,
    "package_root": "prefix",
    "artifacts": files,
    "consumers": {"ctest": "passed", "clean_prefix": "passed"},
}, pathlib.Path(output).open('w', encoding='utf-8'), indent=2)
PY

echo "embedded Unix/Darwin package passed: $BUILD_DIRECTORY/platform-package.json"
