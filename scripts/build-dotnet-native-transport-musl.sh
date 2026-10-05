#!/usr/bin/env sh
set -eu

target=${1:?rust target is required}
output=${2:?output directory is required}

case "$target" in
  x86_64-unknown-linux-musl)
    rid=linux-musl-x64
    ;;
  aarch64-unknown-linux-musl)
    rid=linux-musl-arm64
    ;;
  *)
    echo "unsupported musl target: $target" >&2
    exit 2
    ;;
esac

target_dir=/workspace/target/dotnet-transport
compiler=${MUSL_CC:-}
if [ -z "$compiler" ]; then
  if command -v musl-gcc >/dev/null 2>&1; then
    compiler=musl-gcc
  elif command -v gcc >/dev/null 2>&1; then
    # Alpine's gcc is already configured for its musl libc. The pinned
    # image does not ship the Debian-style musl-gcc wrapper.
    compiler=gcc
  else
    echo "no musl-compatible C compiler found for $target" >&2
    exit 1
  fi
fi
linker_var=CARGO_TARGET_$(printf '%s' "$target" | tr '[:lower:]-' '[:upper:]_')_LINKER
export "$linker_var=$compiler"
export RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=-crt-static"

cargo build \
  --manifest-path /workspace/Cargo.toml \
  -p sdk-dotnet-transport \
  --release \
  --target "$target" \
  --target-dir "$target_dir"

binary="$target_dir/$target/release/libsdk_dotnet_transport.so"
test -f "$binary"
destination="$output/$rid"
mkdir -p "$destination"
cp "$binary" "$destination/libsdk_dotnet_transport.so"
sha256sum "$destination/libsdk_dotnet_transport.so" > "$destination/libsdk_dotnet_transport.so.sha256"
