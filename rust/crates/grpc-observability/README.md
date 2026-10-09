# Shared gRPC observation

Actors and Workers generated unary clients observe their actual typed result in
`acyclic.<family>.grpc.call` at INFO. This includes readiness, request encoding,
response decoding, missing unary messages, and the returned Tonic status. A
never-polled public async method has not started or created a span.

The shared channel records wire/body evidence separately in
`acyclic.<family>.grpc.transport` at TRACE. Canonical explicit trailer status
wins; statusless trailers and EOF retain an explicit header status. Missing
status maps the HTTP status as in pinned Tonic 0.14.6, including HTTP 200 to
Unknown. Header OK stays open through a nonempty body, so a later body failure
or cancellation cannot be reported as success. Tonic can return typed success
from header OK while a contradictory trailer is recorded as a wire error;
these scopes deliberately report different evidence.

One ownership mechanism polls and drops typed futures, transport futures, and
response bodies under the original subscriber and caller. Terminal recording
consumes the owned span once. Filtered operation spans never record fields on
the caller. Only method names, status codes, and fixed outcome/error kinds are
recorded. Raw method attribution uses canonical Tonic `GrpcMethod` metadata;
without it the field stays unset. URI paths, queries, credentials, status
messages/details, and payloads are not copied.

The typed wrapper is inline and adds no heap allocation. The channel retains
one boxed response future and one boxed nonempty body, as the replaced channel
did. Each enabled layer still pays for its span, originating context retention,
poll/drop context entry, and terminal fields. The generator emits one typed
metadata callsite per family. These source facts are not a measured overhead
bound or an optimality claim; subscriber cost, allocation cost, and contention
remain workload-dependent and unmeasured here.

Validation bounds cover pinned Tonic and the tested frame/HTTP/TLS cases, not
all runtimes or all possible subscribers. Tonic's own parsing of status details
and panic behavior remain upstream assumptions; the channel does not invoke
that parser or rewrite consumer data. A separate release must publish this
crate before publishing dependent Actors/Workers packages.

The runtime adapter is native-only; default wasm32 builds of this crate have no
dependencies or runtime observation API. Canonical family producers enable the
optional `codegen` feature only through host build dependencies. Browser clients
retain their uninstrumented target output; their existing Tonic tracing dependency
is unchanged. Alternate native gRPC-web calls have typed observation, while raw
transport spans cover the shared channel path.
