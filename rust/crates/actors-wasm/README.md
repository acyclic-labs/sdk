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

The native companion in `src/native.rs` delegates to the canonical native
Actors client. The shared client owns transport construction, validation, and
typed operation behavior for both browser and native targets.

Cancellation is explicit: calling `CancellationHandle.cancel()` delegates to
the shared Rust `CancellationToken` and returns the canonical `cancelled`
error. A cancelled handle is terminal; create a fresh handle for the next
operation. A transport error is never inferred to be cancellation.
