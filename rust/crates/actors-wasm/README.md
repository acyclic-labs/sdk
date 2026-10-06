# `acyclic-actors-wasm`

This crate is the Rust-owned browser boundary for the Actors v1 contract. It
uses the generated Actors client with the maintained `tonic-web-wasm-client`
transport and exposes the eight protobuf operations as JavaScript bindings.
The public semantic request and response types come from the `ts-rs`
declarations in `acyclic-actors::domain`; this crate only exposes the binary
`Uint8Array` boundary used by the generated facade. Request validation,
protobuf decoding, response encoding, error codes, and message limits stay in
Rust; the JavaScript adapter only passes bytes and an optional
`CancellationHandle`.

The native companion in `src/native.rs` uses the same generated client with a
native tonic channel and bearer interceptor. The generated client must remain
transport-generic (`build_transport(false)`) so browser and native paths share
one Rust client definition.

Cancellation is explicit: calling `CancellationHandle.cancel()` races the
operation with a Rust cancellation signal and returns the canonical gRPC
`cancelled` status. A transport error is never inferred to be cancellation.
