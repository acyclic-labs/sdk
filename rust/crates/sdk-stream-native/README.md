# `acyclic-stream-native`

This private companion crate exposes the canonical Rust remote Stream provider through N-API.
The bridge owns no transport policy of its own: connection setup, endpoint limits, TLS roots,
operation deadlines, retry classification, endpoint rotation, and follow recovery remain in
`acyclic_stream::grpc::Client`.

The exported surface is intentionally byte preserving at the record boundary. Sequence values
are returned as decimal strings so Node and Bun callers cannot lose `u64` precision. A separate
`NativeStreamCancellation` handle wakes a Rust `follow` operation and lets the canonical gRPC
stream be dropped from Rust.

The `npm/` fixtures describe the six platform companion packages expected by a later package
publisher. The native `.node` file is produced by the N-API build for the matching target and is
loaded by each fixture's `index.js`.
