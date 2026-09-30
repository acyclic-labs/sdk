# acyclic-actors

Rust-owned Actors v1 public contract. An Actor has an identity separate from
Streams, an immutable code version, explicit bindings, and independently
recoverable subscriptions. Adding a subscription selects its start once;
delivery and safe cursor advancement are service-owned. Pausing follows a
bounded handler failure, and explicit resumption can replay external effects.
Code and bindings are updated by a full configuration CAS. Checkpoint schema
compatibility or migration is checked before activation; a failed update keeps
the previous version active, and paused subscriptions remain paused.

The service owns execution, fencing, checkpoint storage, and hibernation. This
crate validates customer-authored requests and packages the versioned wire
descriptor used to generate TypeScript bindings.

`grpc::connect(endpoint, token)` exposes every generated Actors service RPC.
Use `grpc::connect_with_ca_certificate` for a caller-supplied private CA.
`http::Client::new(endpoint, token, maximum_response_bytes)` exposes the same
eight operations using canonical Protobuf JSON. Invocation carries request and
response headers. HTTP mutations are not automatically retried.
