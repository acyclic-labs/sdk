# Swift transport producer

This backend runs the maintained SwiftProtobuf and gRPC Swift generators against
verified Rust descriptor exports. It emits a Swift package with public message,
client and server types, exact runtime dependencies, licensing and a hashed
generation receipt. It does not maintain a separate schema or service model.

The admitted generation host is Swift 6.4 on Ubuntu 22.04 x86_64. Toolchain pins
cover both plugin binaries, protoc, the Swift driver/frontend and the Swift
shared libraries loaded by the plugins. Other hosts remain pending admission.
The producer downloads nothing and requires an absent output directory with an
existing parent. Partial output is retained on generation failure.

```sh
node tools/sdk-generator/backends/swift/src/generate.mjs \
  --source-root foundation/source --authority foundation/authority \
  --protoc /absolute/path/to/protoc \
  --swift-plugin /absolute/path/to/protoc-gen-swift \
  --grpc-plugin /absolute/path/to/protoc-gen-grpc-swift-2 \
  --swift-home /absolute/path/to/swift-6.4 \
  --output /absolute/path/to/new-package
node --test tools/sdk-generator/backends/swift/tests/generate.test.mjs
```

Offline staging tests use command doubles and run in affected-backend CI without
installing Swift. Native tool builds and consumer execution are separate local
qualification steps. `tests/fixtures/consumer/Consumer.swift` checks populated
bytes, unsigned maxima, optional zero, oneofs, maintained gRPC codecs and three
RPC metadata entries. It has passed against the source prototype. It does not
yet prove archive installation, all RPC methods or actual client/server calls.
Installed-package qualification, negative compilation controls, native RPC and
Rust-backed RPC remain pending.

`toolchains/build-generators.sh` accepts absolute paths in `SWIFT_HOME`,
`PROTOBUF_SOURCE`, `GRPC_PROTOBUF_SOURCE`, `GRPC_SOURCE`, `COLLECTIONS_SOURCE`
and `BUILD_ROOT`. It verifies clean source revisions, configures package-local
dependency mirrors and builds each generator with one worker. It requires
prepared source checkouts; routine CI does not execute this native build recipe.

The Linux SDK archive signature was valid at signing time (2026-09-14); the
signing key expired on 2026-09-16. Historical admission evidence retains that
distinction. The pinned binaries were built from unchanged SwiftProtobuf
1.38.1 and grpc-swift-protobuf 2.4.1 source, with grpc-swift-2 2.4.3 and
swift-collections 1.7.2 resolved by exact source revision.
