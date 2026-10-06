# Haskell generated SDK

This lane verifies the Haskell SDK path from the Rust contract itself. The
generated package is `acyclic-sdk-haskell` and the Rust contract is the only
input that defines its wire and semantic surface:

1. The contract proto set is copied from the Rust-owned `proto/` projection plus `rust/crates/stream/proto/stream/v2/stream.proto`.
2. `proto-lens-protoc` 0.9.0.1 produces bindings for every active family and archived v1 binding, retaining the generated type-level service method lists.
3. `Acyclic.Stream.Api` maps the generated `StreamService` methods to grapesy `Protobuf` RPC types. `FullTypedMain.hs` consumes the generated `ServiceMethods` type families and proves the active Rust surface contains exactly 106 methods across 18 services at compile time.
4. `run-prototype.ps1` invokes the pinned GHC 9.2.8 and Cabal 3.10.2.1 toolchain from the task cache, compiles all 23 generated binding modules, and runs both the generated wire proof and the type-level 106-method proof.
5. `run-remote-prototype.ps1 -RustGrpcFixture` regenerates from the canonical Rust proto set, builds an installable source archive, installs the archive into an isolated directory, and runs the installed typed consumer against the Rust fixture over HTTP/2.
6. `Acyclic.Semantics` is generated from the resolved Rust descriptor graph. Its 101 known oneof arms are closed GADT constructors with concrete scalar or proto-lens payload types; only the forward-compatible unknown arm accepts raw bytes. The generated semantic test and its negative GHC fixture verify this boundary.

The remote consumer carries a Rust-owned request manifest for `StreamService/Append`. It exercises append and recovery on the live connection, configures grapesy cancellation deadlines and exponential reconnect, and exposes TLS through `ACYCLIC_HASKELL_GRPC_TLS` for a TLS endpoint. The remote fixture run remains separate from the local 106-method type proof so a fixture build failure cannot be mistaken for a type-surface pass. `generate.ps1` is the Rust-owned producer entrypoint and emits the final package identity and provenance alongside the generated source.

Pinned OSS sources:

- @@BT@@grapesy@@BT@@ 1.2.1 (BSD-3-Clause), gRPC client/server and TLS/reconnection/cancellation primitives.
- @@BT@@proto-lens@@BT@@ 0.7.1.7 plus @@BT@@proto-lens-protobuf-types@@BT@@ 0.7.2.3 and @@BT@@proto-lens-runtime@@BT@@ 0.7.0.8.
- @@BT@@proto-lens-protoc@@BT@@ 0.9.0.1.
- @@BT@@http2-grpc-proto-lens@@BT@@ 0.1.1.0 remains the alternative encoder stack for the older http2-grpc-native implementation; it is not mixed into this grapesy prototype.

@@BT@@cabal.project.freeze@@BT@@ records the resolved package graph from the clean GHC 9.2.8 build. @@BT@@hackage-root.json@@BT@@ records the signed Hackage root keys and threshold, while @@BT@@provenance.json@@BT@@ records the Rust proto hash and archive hashes. The runners copy the project to a writable Linux task cache before regeneration and compilation, avoiding mounted filesystem extraction faults. Secure Hackage metadata remains enabled; signature verification is never disabled.
