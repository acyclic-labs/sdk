# acyclic-actors

Actors run versioned code with stateful subscriptions to Streams. Each Actor
has its own identity, immutable code version, explicit bindings, and
independently recoverable subscriptions. Adding a subscription selects its start once;
delivery and safe cursor advancement are service-owned. Pausing follows a
bounded handler failure, and explicit resumption can replay external effects.
Code and bindings are updated by a full configuration CAS. Checkpoint schema
compatibility or migration is checked before activation; a failed update keeps
the previous version active, and paused subscriptions remain paused.

The service manages execution, fencing, checkpoint storage, and hibernation.

`connect(endpoint, token)` creates an authenticated client for all eight
operations. It automatically uses gRPC on native platforms and Protobuf JSON
over HTTP in browsers, with the same methods and response types on both.
Private certificate authorities can be configured through
`grpc::connect_with_ca_certificate`. Invocation carries request and
response headers. HTTP mutations are not automatically retried.
