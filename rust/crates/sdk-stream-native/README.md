# `acyclic-stream-native`

This private companion crate exposes the canonical Rust remote Stream provider through N-API.
The bridge owns no transport policy of its own: connection setup, endpoint limits, TLS roots,
operation deadlines, retry classification, endpoint rotation, and follow recovery remain in
`acyclic_stream::grpc::Client`.

The exported surface is intentionally byte preserving at the record boundary. Sequence values
are returned as decimal strings so Node and Bun callers cannot lose `u64` precision. A separate
`NativeStreamCancellation` handle wakes a Rust `follow` operation and lets the canonical gRPC
stream be dropped from Rust.

The `npm/` fixtures describe the eight platform companion packages. The native `.node` file is
produced by the N-API build for the matching target and is loaded by each fixture's `index.js`.
The release builder includes a `BUILD.json` provenance record in every package, binding the
source Git revision, napi-rs pins, Rust target, loader hash, and binary hash.

The checked-in runtime qualification currently builds and loads the Windows x64 module through a
clean `node_modules/@acyclic-labs/stream-win32-x64` consumer. It exercises append, read, follow
cancellation, private-CA TLS, endpoint rotation, and the canonical capability bounds against a
local Rust gRPC service. The release workflow builds and packs all eight targets and verifies the
packed Windows x64 archive in a second clean consumer; its install record is retained beside the
archive.
