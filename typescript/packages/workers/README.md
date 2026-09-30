# @acyclic-labs/workers

Workers v1 generated public contract and authenticated HTTP client. Publish
exact JavaScript module bytes, select a deployment alias by expected revision,
and submit durable jobs. Accepted jobs pin the resolved code digest even if the
alias later changes. Invocation has ordinary HTTP ambiguity and does not
acknowledge a durable job. The live Cloud executor and public route are not yet
qualified against this contract.

An ES module may implement `default.fetch(Request)` for HTTP, `default.run(bytes,
context)` for durable jobs, or both. The package exports Rust-generated
`WorkerModule` and `WorkerJobContext` types. Retried jobs receive the same job ID
and input with a higher attempt number; external effects need application
idempotency.

Object inputs name an S3 bucket and key. The service privately retains the
accepted bytes for retries. Job results contain exact bytes bounded by the
accepted output budget (at most 1 MiB), without public Object version references.

The Rust crate `acyclic-workers` and `proto/workers/v1/workers.proto` own the
contract. Import generated request and response schemas from the package or
`@acyclic-labs/workers/proto`; `HttpWorkersClient` supplies the transport.

For Node/Bun gRPC, import `createWorkersGrpcClient` from
`@acyclic-labs/workers/grpc` and provide `{ endpoint, token }`. Every generated
RPC is exposed. Optional `caCertificate` adds a private PEM CA, and
`maximumMessageBytes` bounds requests and responses. Browser applications use
the HTTP client. Omit `expectedRevision` only to create an absent alias; replacing
an existing selection requires its positive current revision.
