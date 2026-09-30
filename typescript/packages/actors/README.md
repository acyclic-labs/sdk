# @acyclic-labs/actors

Actors v1 generated public contract and authenticated HTTP client. An Actor has
an identity separate from any Stream, immutable code version, explicit bindings,
and independent Stream subscriptions. Subscription delivery is at least once;
safe cursor advancement is service-owned. Resuming a paused subscription may
replay external effects. The Cloud Actor service and public route are not yet
qualified against this contract.
Updating code and bindings replaces the full configuration by expected revision;
it does not advance or rewind subscription cursors. A paused subscription stays
paused until explicitly resumed after a compatible code update or migration.

The Rust crate `acyclic-actors` and `proto/actors/v1/actors.proto` own the
contract. Import generated request and response schemas from the package or
`@acyclic-labs/actors/proto`; `HttpActorsClient` supplies the transport.

For Node/Bun gRPC, import `createActorsGrpcClient` from
`@acyclic-labs/actors/grpc` and provide `{ endpoint, token }`. Every generated
RPC is exposed. Optional `caCertificate` adds a private PEM CA, and
`maximumMessageBytes` bounds requests and responses. Browser applications use
the HTTP client. Actor invocation includes request and response headers.
