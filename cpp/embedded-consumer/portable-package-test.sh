#!/usr/bin/env bash
set -euo pipefail

# Cross-platform release recipe for the Rust-owned embedded ABI. The Windows
# lane uses portable-package-test.ps1; Unix and Darwin lanes use this script
# with their native runner toolchain. No platform feature flag is passed to
# Cargo: the target and linker are supplied by the release runner.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_TARGET="${RUST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
BUILD_DIRECTORY="${BUILD_DIRECTORY:-${RUNNER_TEMP:-$ROOT}/acyclic-embedded-$RUST_TARGET}"
CXX="${CXX:-c++}"
CC="${CC:-cc}"

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
command -v rustc >/dev/null
command -v cmake >/dev/null
command -v ctest >/dev/null
command -v ninja >/dev/null
command -v "$CXX" >/dev/null
command -v "$CC" >/dev/null
command -v python3 >/dev/null

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

INSTALLED_RUNTIME="$(find "$PREFIX" -type f -name "$RUNTIME_NAME" -print -quit)"
INSTALLED_HEADER="$PREFIX/include/acyclic_embedded_prototype.h"
test -f "$INSTALLED_RUNTIME"
test -f "$INSTALLED_HEADER"
INSTALLED_LIBRARY_DIRECTORY="$(dirname "$INSTALLED_RUNTIME")"
C_CONSUMER="$BUILD_DIRECTORY/c-consumer"
"$CC" -std=c11 -I"$PREFIX/include" \
  "$ROOT/rust/crates/sdk-embedded-prototype/tests/c_consumer.c" \
  "$INSTALLED_RUNTIME" -o "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" \
  python3 "$ROOT/rust/crates/sdk-embedded-prototype/tests/python_consumer.py" "$INSTALLED_RUNTIME"

cmake -S "$CONSUMER_SOURCE/install-consumer" -B "$INSTALLED_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="$CXX" -DCMAKE_PREFIX_PATH="$PREFIX"
cmake --build "$INSTALLED_BUILD"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"

python3 - "$BUILD_DIRECTORY/platform-package.json" "$PREFIX" "$RUST_TARGET" "$RUNTIME_NAME" "$ROOT" "$INSTALLED_RUNTIME" <<'PY'
import hashlib
import json
import pathlib
import sys

output, prefix, target, runtime, source_root, runtime_path = sys.argv[1:]
root = pathlib.Path(prefix)
source_root = pathlib.Path(source_root)
import subprocess
source_revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source_root, text=True).strip()
runtime_package_path = pathlib.Path(runtime_path).relative_to(root).as_posix()
source_inputs = [
    "rust/crates/sdk-embedded-prototype/Cargo.toml",
    "rust/crates/sdk-embedded-prototype/Cargo.lock",
    "rust/crates/sdk-embedded-prototype/build.rs",
    "rust/crates/sdk-embedded-prototype/src/lib.rs",
    "rust/crates/sdk-embedded-prototype/tests/c_consumer.c",
    "rust/crates/sdk-embedded-prototype/tests/python_consumer.py",
    "cpp/embedded-consumer/CMakeLists.txt",
    "cpp/embedded-consumer/AcyclicEmbeddedConfig.cmake.in",
    "cpp/embedded-consumer/include/acyclic/embedded.hpp",
    "cpp/embedded-consumer/main.cpp",
    "cpp/embedded-consumer/negative_lifetime_smoke.cpp",
    "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp",
]
source_digest_lines = []
for relative in source_inputs:
    source_digest_lines.append(
        f"{relative} {hashlib.sha256((source_root / relative).read_bytes()).hexdigest()}"
    )
source_digest = hashlib.sha256("\n".join(source_digest_lines).encode()).hexdigest()
files = {}
for path in sorted(p for p in root.rglob('*') if p.is_file()):
    files[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
json.dump({
    "schema": "acyclic.sdk.embedded.platform-package.v1",
    "status": "passed",
    "target": target,
    "source_revision": source_revision,
    "source_digest": f"sha256:{source_digest}",
    "source_inputs": source_inputs,
    "runtime": runtime,
    "runtime_artifact": runtime_package_path,
    "package_root": "prefix",
    "artifacts": files,
    "consumers": {
        "c": {
            "status": "passed", "scope": "embedded-native-abi", "invoked": True, "exit_code": 0,
            "source_revision": source_revision,
            "source": "rust/crates/sdk-embedded-prototype/tests/c_consumer.c",
            "source_sha256": hashlib.sha256((source_root / "rust/crates/sdk-embedded-prototype/tests/c_consumer.c").read_bytes()).hexdigest(),
            "package_artifact": runtime_package_path,
            "package_artifact_sha256": hashlib.sha256(pathlib.Path(runtime_path).read_bytes()).hexdigest(),
            "checks": ["layout", "append", "read", "release", "stale_handles"],
        },
        "python": {
            "status": "passed", "scope": "embedded-native-abi", "invoked": True, "exit_code": 0,
            "source_revision": source_revision,
            "source": "rust/crates/sdk-embedded-prototype/tests/python_consumer.py",
            "source_sha256": hashlib.sha256((source_root / "rust/crates/sdk-embedded-prototype/tests/python_consumer.py").read_bytes()).hexdigest(),
            "package_artifact": runtime_package_path,
            "package_artifact_sha256": hashlib.sha256(pathlib.Path(runtime_path).read_bytes()).hexdigest(),
            "checks": ["append", "follow", "owned_buffers", "cancel", "stale_handles"],
        },
        "cpp": {
            "status": "passed", "scope": "embedded-native-abi", "invoked": True, "exit_code": 0,
            "source_revision": source_revision,
            "source": "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp",
            "source_sha256": hashlib.sha256((source_root / "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp").read_bytes()).hexdigest(),
            "package_artifact": runtime_package_path,
            "package_artifact_sha256": hashlib.sha256(pathlib.Path(runtime_path).read_bytes()).hexdigest(),
            "checks": ["blocked_pull_wakeup", "cross_thread_cancel", "clean_prefix_install"],
        },
        "ctest": "passed", "clean_prefix": "passed"
    },
}, pathlib.Path(output).open('w', encoding='utf-8'), indent=2)
PY

echo "embedded Unix/Darwin package passed: $BUILD_DIRECTORY/platform-package.json"
