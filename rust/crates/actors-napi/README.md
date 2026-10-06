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
