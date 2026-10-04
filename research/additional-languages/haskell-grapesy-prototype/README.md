# Haskell generated SDK prototype

This lane verifies the Haskell OSS path from the Rust contract itself:

1. `source/stream.proto` is copied from `rust/crates/stream/proto/stream/v2/stream.proto`.
2. `proto-lens-protoc` 0.9.0.1 produces `generated/Proto/Stream/V2/Stream.hs` and its field module.
3. `Acyclic.Stream.Api` maps the generated `StreamService` methods to grapesy `Protobuf` RPC types. It contains endpoint type aliases only; shared request/response behavior stays in the generated module and grapesy.
4. `run-prototype.ps1` invokes the pinned GHC 9.2.8 and Cabal 3.10.2.1 toolchain from the task cache and runs the generated wire proof.

The prototype is intentionally a build and serialization qualification. The current Rust Stream service fixture exposes HTTP and gRPC surfaces separately; an actual remote H2/TLS run is enabled by extending the same generated service aliases with the fixture endpoint and belongs in release/manual CI until the fixture host is available.

Pinned OSS sources:

- `grapesy` 1.2.1 (BSD-3-Clause), gRPC client/server and TLS/reconnection/cancellation primitives.
- `proto-lens` 0.7.1.7 plus `proto-lens-protobuf-types` 0.7.2.3 and `proto-lens-runtime` 0.7.0.8.
- `proto-lens-protoc` 0.9.0.1.
- `http2-grpc-proto-lens` 0.1.1.0 remains the alternative encoder stack for the older http2-grpc-native implementation; it is not mixed into this grapesy prototype.

`cabal.project.freeze` records the resolved package graph from the clean GHC 9.2.8 build. `hackage-root.json` records the signed Hackage root keys and threshold, while `provenance.json` records the Rust proto hash and archive hashes. The runner copies the project to a writable Linux task cache before regeneration and compilation, avoiding mounted filesystem extraction faults. Secure Hackage metadata remains enabled; signature verification is never disabled.
