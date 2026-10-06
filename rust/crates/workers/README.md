# acyclic-workers

Workers run immutable code versions through HTTP invocations and durable jobs.
Publish a version, select a deployment alias, and submit work through one client.
Code versions are SHA-256 identities; creating an absent deployment alias omits the expected revision.
Changing an existing alias requires its positive current revision. Each accepted
selection advances the revision.
An accepted durable job pins its resolved code version. HTTP invocation has
ordinary request ambiguity and does not imply durable job acceptance.

A published ES module may export `default.fetch(Request)` for HTTP invocation,
`default.run(Uint8Array, { jobId, attempt, signal })` for durable jobs, or both.
`run` receives the accepted input for every retry with the same job ID; attempt
numbers start at one. Its byte result is published once under attempt fencing.
Cancellation signals the handler but cannot undo external effects.

Object inputs name an S3 bucket and key. Acceptance privately retains the
selected immutable bytes so key replacement cannot change a retry's input.
Job observations return an exact `JobResult` body bounded by the accepted output
budget (at most 1 MiB); they expose no mutable object pointer or public Object
version. The service owns durable acceptance, retention and attempt fencing.

`connect(endpoint, token)` creates an authenticated client for all seven
operations. It automatically uses gRPC on native platforms and Protobuf JSON
over HTTP in browsers, with the same methods and response types on both.
Private certificate authorities can be configured through
`grpc::connect_with_ca_certificate`. Version invocation addresses the
exact digest; alias invocation reports the version and revision resolved by the
service. HTTP mutations are not automatically retried.
