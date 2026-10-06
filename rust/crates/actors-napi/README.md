# Actors native binding

This private N-API crate exposes the canonical `acyclic-actors::client::Client`.
Protobuf request and response bytes cross the JavaScript boundary; endpoint
selection, authentication, transport policy, limits, and operation semantics
remain in the Rust Actors client. Each operation accepts an optional
`NativeActorsCancellation` whose cancellation drops the pending Rust future.
