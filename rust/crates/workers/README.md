# acyclic-workers

Rust-owned Workers v1 public contract. Code versions are immutable SHA-256
identities; creating an absent deployment alias omits the expected revision.
Changing an existing alias requires its positive current revision. Each accepted
selection advances the revision.
An accepted durable job pins its resolved code version. HTTP invocation has
ordinary request ambiguity and does not imply durable job acceptance.

A published ES module may export `default.fetch(Request)` for HTTP invocation,
`default.run(Uint8Array, { jobId, attempt, signal })` for durable jobs, or both.
`run` receives the accepted input for every retry with the same job ID; attempt
numbers start at one. Its byte result is published once under attempt fencing.
Cancellation signals the handler but cannot undo external effects.

The service implementation and execution authority live outside this crate.
This crate validates customer-authored requests and packages the versioned wire
descriptor used to generate TypeScript bindings.

`grpc::connect(endpoint, token)` exposes every generated Workers service RPC.
Use `grpc::connect_with_ca_certificate` for a caller-supplied private CA.
`http::Client::new(endpoint, token, maximum_response_bytes)` exposes the same
seven operations using canonical Protobuf JSON. Version invocation addresses the
exact digest; alias invocation reports the version and revision resolved by the
service. HTTP mutations are not automatically retried.
