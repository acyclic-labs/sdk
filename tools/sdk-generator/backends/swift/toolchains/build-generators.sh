#!/bin/sh
# Build from caller-owned, unchanged source checkouts. No network acquisition.
set -eu
: "${SWIFT_HOME:?}" "${PROTOBUF_SOURCE:?}" "${GRPC_PROTOBUF_SOURCE:?}" "${GRPC_SOURCE:?}" "${COLLECTIONS_SOURCE:?}" "${BUILD_ROOT:?}"
verify_revision() {
  test "$(git -C "$1" rev-parse HEAD)" = "$2"
  test -z "$(git -C "$1" status --porcelain --untracked-files=all)"
}
verify_revision "$PROTOBUF_SOURCE" 55d7a1cc5666b85c13464aea1c4b4a90feccb4c8
verify_revision "$GRPC_PROTOBUF_SOURCE" 176c5a434fd76f6f479848d1a8f7d44967534168
verify_revision "$GRPC_SOURCE" ac33066eb6edb1a21a6ca172ea8184a9b06f3cc7
verify_revision "$COLLECTIONS_SOURCE" 3b69cedbaa49957e97c092ae400bb57602ef941f
swift="$SWIFT_HOME/usr/bin/swift"
mkdir -p "$BUILD_ROOT"
"$swift" build --package-path "$PROTOBUF_SOURCE" --scratch-path "$BUILD_ROOT/protobuf" \
  --cache-path "$BUILD_ROOT/cache" --config-path "$BUILD_ROOT/config" \
  --security-path "$BUILD_ROOT/security" --configuration release --product protoc-gen-swift --jobs 1
mirror() {
  "$swift" package --package-path "$GRPC_PROTOBUF_SOURCE" --config-path "$BUILD_ROOT/config" \
    --cache-path "$BUILD_ROOT/cache" config set-mirror --original "$1" --mirror "file://$2"
}
mirror https://github.com/apple/swift-protobuf.git "$PROTOBUF_SOURCE"
mirror https://github.com/grpc/grpc-swift-2.git "$GRPC_SOURCE"
mirror https://github.com/apple/swift-collections.git "$COLLECTIONS_SOURCE"
"$swift" build --package-path "$GRPC_PROTOBUF_SOURCE" --scratch-path "$BUILD_ROOT/grpc-protobuf" \
  --cache-path "$BUILD_ROOT/cache" --config-path "$BUILD_ROOT/config" \
  --security-path "$BUILD_ROOT/security" --configuration release --product protoc-gen-grpc-swift-2 --jobs 1
