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
RPC metadata entries. It has passed against both the source prototype and an
archive-installed package. Three independent invalid assignments to Actor bytes,
Worker bytes and Stream optional integers fail with the intended compiler type
errors against that installed package. All four compiled dependency source trees
match their exact admitted Git revisions.

`src/package.mjs` admits the archive against its external checksum, complete
producer receipt, maintained source/tool pins and verified Rust authority.
Offline controls reject unsafe members, links, duplicate paths, missing bindings,
payload changes and substituted package metadata. Admission does not execute
native consumers. `RPCConsumer.swift` has separately passed all 25 generated
client/server calls over the maintained in-process gRPC transport, using Rust
descriptors for populated field values, type names, paths and streaming shapes.
This is native in-process transport evidence; network and Rust-backed RPC
qualification remain pending.

`toolchains/qualification.json` pins the full SDK inventory artifact and exact
dependency revisions. The inventory contains 2,317 files and 39 symlinks and has
been compared with the checksum-verified official SDK archive. The inventory,
SDK downloads and native build outputs remain outside the maintained source tree.

The reusable installed workflow in `src/qualify.mjs` stages only admitted archive
bytes, creates isolated local dependency mirrors, builds with one worker, runs
both consumer fixtures, checks three negative compilations and verifies input
provenance again. Git HTTP/HTTPS transports are disabled. Its offline command
controls are covered by CI; a real end-to-end invocation is being qualified.

```sh
node tools/sdk-generator/backends/swift/src/qualify.mjs \
  --package package.tar.gz --sha256 <archive-sha256> \
  --receipt generation-receipt.json --authority foundation/authority \
  --swift-home /absolute/path/to/swift-6.4 \
  --sdk-inventory /absolute/path/to/admitted-sdk-inventory.json \
  --dependencies /absolute/path/to/prepared-source-checkouts \
  --git /usr/bin/git --output /absolute/path/to/new-qualification
```

The dependency directory contains the four checkouts named in
`toolchains/qualification.json`, at their exact revisions and version tags.
The SDK inventory is an external artifact verified by its maintained checksum.
Qualification output must be absent, with an existing parent. A failed native
operation retains logs and partial output and does not emit a success receipt.

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
