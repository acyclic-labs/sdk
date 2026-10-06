#!/usr/bin/env bash
set -euo pipefail

# Cross-platform release recipe for the Rust-owned embedded ABI. The Windows
# lane uses portable-package-test.ps1; Unix and Darwin lanes use this script
# with their native runner toolchain. No platform feature flag is passed to
# Cargo: the target and linker are supplied by the release runner.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_TARGET="${RUST_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
BUILD_DIRECTORY="${BUILD_DIRECTORY:-${RUNNER_TEMP:-$ROOT}/acyclic-embedded-$RUST_TARGET}"
# Resolve the common relative form before creating anything.  A package run
# must never put Cargo, CMake, or receipt output into the Rust source checkout.
if [[ "$BUILD_DIRECTORY" != /* ]]; then
  BUILD_DIRECTORY="$PWD/$BUILD_DIRECTORY"
fi
case "$BUILD_DIRECTORY" in
  "$ROOT"|"$ROOT"/*)
    echo "embedded package output must be outside the source checkout: $BUILD_DIRECTORY" >&2
    exit 2
    ;;
esac
CXX="${CXX:-c++}"
CC="${CC:-cc}"

# Rust's Linux musl target defaults to crt-static, which suppresses cdylib
# output.  Embedded packages intentionally ship a shared C ABI, so request
# the dynamic musl CRT from the producer.  Keep this entirely inside the
# package recipe; consumers never need a Rust feature flag.
MUSL_DYNAMIC=0
if [[ "$RUST_TARGET" == *-unknown-linux-musl ]]; then
  MUSL_DYNAMIC=1
  musl_compiler_for_target() {
    local candidate machine
    for candidate in "$@"; do
      [[ -n "$candidate" ]] || continue
      command -v "$candidate" >/dev/null 2>&1 || continue
      machine="$($candidate -dumpmachine 2>/dev/null || true)"
      [[ "$machine" == *musl* ]] || continue
      printf '%s' "$(command -v "$candidate")"
      return 0
    done
    return 1
  }
  case "$RUST_TARGET" in
    x86_64-unknown-linux-musl)
      MUSL_CC="${MUSL_CC:-$(musl_compiler_for_target x86_64-linux-musl-gcc musl-gcc gcc || true)}"
      ;;
    aarch64-unknown-linux-musl)
      MUSL_CC="${MUSL_CC:-$(musl_compiler_for_target aarch64-linux-musl-gcc musl-gcc gcc || true)}"
      ;;
    *)
      echo "unsupported musl target: $RUST_TARGET" >&2
      exit 2
      ;;
  esac
  if [[ -z "$MUSL_CC" ]]; then
    echo "no pinned musl C compiler is available for $RUST_TARGET" >&2
    exit 2
  fi
  export MUSL_CC
  export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-C target-feature=-crt-static"
  target_env="${RUST_TARGET//-/_}"
  export "CC_${target_env}=$MUSL_CC"
  export "CARGO_TARGET_${target_env^^}_LINKER=$MUSL_CC"
  # The embedded C++ consumers only use the C ABI and C headers.  Alpine's
  # musl-gcc can compile these translation units without introducing a glibc
  # linker; a dedicated MUSL_CXX may be supplied by a pinned cross toolchain.
  CXX="${MUSL_CXX:-$MUSL_CC}"
  CC="$MUSL_CC"
fi

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

CMAKE_PLATFORM_ARGS=()
C_CONSUMER_PLATFORM_ARGS=()
if [[ -n "${CMAKE_OSX_ARCHITECTURES:-}" ]]; then
  CMAKE_PLATFORM_ARGS+=("-DCMAKE_OSX_ARCHITECTURES=$CMAKE_OSX_ARCHITECTURES")
  # The installed C consumer is compiled directly rather than through CMake;
  # keep its architecture identical to the package and CMake consumers when a
  # macOS x64 lane runs on an Apple Silicon host.
  C_CONSUMER_PLATFORM_ARGS+=("-arch" "$CMAKE_OSX_ARCHITECTURES")
fi

assert_darwin_architecture() {
  [[ -n "${CMAKE_OSX_ARCHITECTURES:-}" ]] || return 0
  command -v lipo >/dev/null || {
    echo "lipo is required to verify the Darwin package architecture" >&2
    exit 2
  }
  local actual
  actual="$(lipo -archs "$1")"
  if [[ "$actual" != *"$CMAKE_OSX_ARCHITECTURES"* ]]; then
    echo "Darwin runtime architecture mismatch: expected $CMAKE_OSX_ARCHITECTURES, got '$actual' for $1" >&2
    exit 2
  fi
}

mkdir -p "$BUILD_DIRECTORY"
command -v cargo >/dev/null
command -v rustc >/dev/null
command -v cmake >/dev/null
command -v ctest >/dev/null
command -v ninja >/dev/null
command -v "$CXX" >/dev/null
command -v "$CC" >/dev/null
command -v python3 >/dev/null

# The x64 macOS package is intentionally built on the shared ARM64 runner.
# Run its Python consumer through Rosetta so the process architecture matches
# the dylib being tested; loading an x86_64 dylib in host ARM64 Python fails
# before any ABI checks execute.
PYTHON_RUNNER=(python3)
if [[ "$(uname -s)" == "Darwin" && "${CMAKE_OSX_ARCHITECTURES:-}" == "x86_64" && "$(uname -m)" == "arm64" ]]; then
  command -v arch >/dev/null
  PYTHON_RUNNER=(arch -x86_64 python3)
fi
if command -v rustup >/dev/null 2>&1; then
  rustup target list --installed | grep -Fx "$RUST_TARGET" >/dev/null || {
    echo "Rust target is not installed: $RUST_TARGET" >&2
    exit 2
  }
fi
test -f "$MANIFEST"

cargo build --locked --offline --release --target "$RUST_TARGET" \
  --target-dir "$TARGET_DIRECTORY" --manifest-path "$MANIFEST"

RUNTIME="$RELEASE_DIRECTORY/$RUNTIME_NAME"
HEADER="$(find "$RELEASE_DIRECTORY/build" -type f -name acyclic_embedded_prototype.h -print -quit)"
test -f "$RUNTIME"
test -n "$HEADER"
assert_darwin_architecture "$RUNTIME"
if (( MUSL_DYNAMIC )); then
  command -v readelf >/dev/null || {
    echo "readelf is required to verify the dynamic musl ABI" >&2
    exit 2
  }
  readelf -d "$RUNTIME" | grep -Eq 'Shared library: \[libc\.so\]' || {
    echo "musl runtime is not dynamically linked to musl libc: $RUNTIME" >&2
    exit 2
  }
fi

assert_musl_executable() {
  if (( MUSL_DYNAMIC )) && ! readelf -l "$1" | grep -Eq 'Requesting program interpreter: .*/ld-musl([^/]*)?\.so\.1'; then
    echo "consumer is not a musl executable: $1" >&2
    exit 2
  fi
}

cmake -S "$CONSUMER_SOURCE" -B "$CONSUMER_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DACYCLIC_EMBEDDED_ROOT="$RELEASE_DIRECTORY" \
  -DACYCLIC_EMBEDDED_HEADER="$HEADER" \
  "${CMAKE_PLATFORM_ARGS[@]}"
cmake --build "$CONSUMER_BUILD"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_consumer"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_negative"
assert_musl_executable "$CONSUMER_BUILD/acyclic_cpp_embedded_cross_thread"
ctest --test-dir "$CONSUMER_BUILD" --output-on-failure
cmake --install "$CONSUMER_BUILD" --prefix "$PREFIX"

INSTALLED_RUNTIME="$(find "$PREFIX" -type f -name "$RUNTIME_NAME" -print -quit)"
INSTALLED_HEADER="$PREFIX/include/acyclic_embedded_prototype.h"
test -f "$INSTALLED_RUNTIME"
test -f "$INSTALLED_HEADER"
assert_darwin_architecture "$INSTALLED_RUNTIME"
"${PYTHON_RUNNER[@]}" - "$PREFIX" "$RUNTIME_NAME" <<'PY'
import pathlib
import sys

prefix = pathlib.Path(sys.argv[1])
runtime_name = sys.argv[2]
expected = {
    "include/acyclic/embedded.hpp",
    "include/acyclic_embedded_prototype.h",
    f"lib/{runtime_name}",
    "lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfig.cmake",
    "lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfigVersion.cmake",
}
actual = {
    path.relative_to(prefix).as_posix()
    for path in prefix.rglob("*")
    if path.is_file()
}
if actual != expected:
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    raise SystemExit(f"installed package file set mismatch: missing={missing} extra={extra}")
PY
INSTALLED_LIBRARY_DIRECTORY="$(dirname "$INSTALLED_RUNTIME")"
C_CONSUMER="$BUILD_DIRECTORY/c-consumer"
"$CC" "${C_CONSUMER_PLATFORM_ARGS[@]}" -std=c11 -I"$PREFIX/include" \
  "$ROOT/rust/crates/sdk-embedded-prototype/tests/c_consumer.c" \
  "$INSTALLED_RUNTIME" -o "$C_CONSUMER"
assert_musl_executable "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$C_CONSUMER"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" \
    "${PYTHON_RUNNER[@]}" "$ROOT/rust/crates/sdk-embedded-prototype/tests/python_consumer.py" "$INSTALLED_RUNTIME"

cmake -S "$CONSUMER_SOURCE/install-consumer" -B "$INSTALLED_BUILD" -G Ninja \
  -DCMAKE_CXX_COMPILER="$CXX" -DCMAKE_PREFIX_PATH="$PREFIX" \
  "${CMAKE_PLATFORM_ARGS[@]}"
cmake --build "$INSTALLED_BUILD"
assert_musl_executable "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"
env "$RUNTIME_ENV=$INSTALLED_LIBRARY_DIRECTORY" "$INSTALLED_BUILD/acyclic_cpp_installed_consumer"

"${PYTHON_RUNNER[@]}" - "$BUILD_DIRECTORY/platform-package.json" "$PREFIX" "$RUST_TARGET" "$RUNTIME_NAME" "$ROOT" "$INSTALLED_RUNTIME" <<'PY'
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
    "rust/crates/sdk-embedded-prototype/src/uniffi_polling.rs",
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
        },
    "ctest": "passed",
    "clean_prefix": "passed"
}, pathlib.Path(output).open('w', encoding='utf-8'), indent=2)
PY

echo "embedded Unix/Darwin package passed: $BUILD_DIRECTORY/platform-package.json"
