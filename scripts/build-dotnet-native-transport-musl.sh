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
export CARGO_TARGET_$(printf '%s' "$target" | tr '[:lower:]-' '[:upper:]_')_LINKER=musl-gcc
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
