# `acyclic-inference-native`

This private companion crate exposes the canonical Rust Inference client through
N-API. Requests and responses remain protobuf bytes at the JavaScript boundary;
transport selection, authenticated handshakes, response limits, and operation
semantics remain owned by `acyclic_inference::client::Client`.

The `npm/` directory contains the platform companion package fixtures. A native
`.node` binary is produced for the matching target by the package publisher.
