# Actors native binding

This private N-API crate exposes the canonical `acyclic-actors::client::Client`.
Protobuf request and response bytes cross the JavaScript boundary; endpoint
selection, authentication, transport policy, limits, and operation semantics
remain in the Rust Actors client. Each operation accepts an optional
`NativeActorsCancellation`. Cancellation is monotonic: it interrupts the
pending Rust future, and a fresh handle is required for a later operation.
Requests are decoded from protobuf bytes into the Rust domain types before
dispatch, and responses are converted back to protobuf bytes only after the
domain conversion succeeds. Operation failures use the structured result
metadata (`code`, numeric `grpcCode` with `grpcName`, `serviceCode` and
`serviceMessage`, plus semantic `semanticCode`, `semanticValue`, and
`contractCode` fields where applicable).

Connection failures use the same generated metadata through the
`connectResult(endpoint, token, cancellation?)` and
`connectWithCaResult(endpoint, token, caCertificatePem, cancellation?)`
factories. These methods resolve to `{ client, error }`, with exactly one
field populated, so the TypeScript facade does not decode an N-API error
message or maintain a second error schema. The legacy `connect` and
`connectWithCa` factories remain available for existing consumers.

The `ActorId(value)` and `CodeSha256(value)` exports are Rust-owned validating
constructors. Their generated declarations reference the canonical
`@acyclic-labs/actors/types` aliases, so a TypeScript facade can use the same
nominal types as the `ts-rs` domain declarations without a second runtime
schema or JavaScript-side validator.
