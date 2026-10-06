# ScalaPB gRPC prototype

This is the independent protobuf/gRPC target prototype complementary to the
Scala HTTP projection. It uses [ScalaPB](https://github.com/scalapb/ScalaPB)
`0.11.17`, `scalapb-runtime-grpc` `0.11.17`, and `sbt-protoc` `1.0.7` to
generate Scala messages and service bindings from the Rust authority export.
The export directory contains the Rust-emitted `.proto` files and
`rust-authority.json`; its `source_revision` is copied into the qualification
receipt. It runs `compile`, `package`, and isolated `publishLocal` with sbt
`1.10.11`.

The run generated 70 Scala sources from the Rust-owned Actors and Stream
protobuf fixtures and produced the local artifact
`dev.acyclic:acyclic-sdk-scala-grpc-prototype_2.13:0.1.0` with SHA-256
`E5D6385E482CC242D845BDF1CAD4A04E3A674AB52996E2D5CB2AFA533526155A`.

The generated consumer exercises the Rust tonic fixture with bearer metadata,
bytes, `uint64`, proto3 optional presence, and a server stream. A separate
installed consumer resolves the published artifact from an isolated Ivy cache
and passes Actors and Stream append/read, idempotency replay, and cancellation
vectors over network gRPC.

Run [`scala-grpc-prototype.ps1`](scala-grpc-prototype.ps1) from the SDK root
with `-RustGrpcFixture -AuthorityRoot <rust-authority-export>`. The script
builds and starts the bounded Rust-owned tonic fixture from
`rust/crates/sdk-examples/src/bin/fixture-server.rs`, then send the generated
Actors and Stream calls over loopback gRPC. That mode records
`rust_fixture: true` in the receipt and makes no hosted service availability
claim.
Pass `-SourceRevision <git-sha>` when the producer is run from a staged
checkout; otherwise the script records the root checkout's exact Git revision.
The script writes the tracked `scala-receipt.json` and an ignored
`target/scala-grpc/scala-grpc-receipt.json`, binding every input proto and
generated Scala source hash to the artifact and installed-consumer result. The
artifact is local-only and is not published to a registry. This prototype
proves native Scala protobuf/gRPC generation and installed package wiring; full
target qualification still requires the descriptor digest, custom option,
metadata, recovery and shared conformance gates.
