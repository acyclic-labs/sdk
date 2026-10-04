# Haskell generated SDK prototype

This lane verifies the Haskell OSS path from the Rust contract itself:

1. @@BT@@source/stream.proto@@BT@@ is copied from @@BT@@rust/crates/stream/proto/stream/v2/stream.proto@@BT@@.
2. @@BT@@proto-lens-protoc@@BT@@ 0.9.0.1 produces @@BT@@generated/Proto/Stream/V2/Stream.hs@@BT@@ and its field module.
3. @@BT@@Acyclic.Stream.Api@@BT@@ maps the generated @@BT@@StreamService@@BT@@ methods to grapesy @@BT@@Protobuf@@BT@@ RPC types. Shared request and response behavior stays in generated bindings and grapesy.
4. @@BT@@run-prototype.ps1@@BT@@ invokes the pinned GHC 9.2.8 and Cabal 3.10.2.1 toolchain from the task cache and runs the generated wire proof.
5. @@BT@@run-remote-prototype.ps1 -RustGrpcFixture@@BT@@ regenerates from the canonical Rust proto, builds an installable source archive, installs the archive into an isolated directory, and runs the installed typed consumer against the Rust fixture over HTTP/2.

The remote consumer carries a Rust-owned request manifest for @@BT@@StreamService/Append@@BT@@. It exercises append and recovery on the live connection, configures grapesy cancellation deadlines and exponential reconnect, and exposes TLS through @@BT@@ACYCLIC_HASKELL_GRPC_TLS@@BT@@ for a TLS endpoint.

Pinned OSS sources:

- @@BT@@grapesy@@BT@@ 1.2.1 (BSD-3-Clause), gRPC client/server and TLS/reconnection/cancellation primitives.
- @@BT@@proto-lens@@BT@@ 0.7.1.7 plus @@BT@@proto-lens-protobuf-types@@BT@@ 0.7.2.3 and @@BT@@proto-lens-runtime@@BT@@ 0.7.0.8.
- @@BT@@proto-lens-protoc@@BT@@ 0.9.0.1.
- @@BT@@http2-grpc-proto-lens@@BT@@ 0.1.1.0 remains the alternative encoder stack for the older http2-grpc-native implementation; it is not mixed into this grapesy prototype.

@@BT@@cabal.project.freeze@@BT@@ records the resolved package graph from the clean GHC 9.2.8 build. @@BT@@hackage-root.json@@BT@@ records the signed Hackage root keys and threshold, while @@BT@@provenance.json@@BT@@ records the Rust proto hash and archive hashes. The runners copy the project to a writable Linux task cache before regeneration and compilation, avoiding mounted filesystem extraction faults. Secure Hackage metadata remains enabled; signature verification is never disabled.

