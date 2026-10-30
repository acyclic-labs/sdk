# acyclic-objects

Content-addressed, account-scoped immutable blobs.

- A blob's digest is BLAKE3-256 of its logical bytes. `put` returns
  `Created`, `AlreadyExists` (verified against this account) or `Unknown`
  (outcome ambiguous; retrying is safe).
- `get(digest, range, maximum)` reads a half-open logical range; `head`
  returns kind, length and references.
- A manifest lists `(digest, length)` parts plus typed edges; its digest is
  BLAKE3 of the canonical encoding and reads select only intersecting parts.
- References live in the envelope, outside the payload. `retain(root, refs)`
  appends one atomic, idempotent batch; `release(root)` drops the root;
  `collect` marks from every root and sweeps the rest. Delete never breaks a
  live reference: it returns `Referenced`.
- `list(after, limit)` pages digests in strict ascending order.
- `usage` reports the account's blob count and unique bytes.

One engine (`Store<M: Medium>`) serves memory, local-directory (`local`) and
S3-compatible (`s3`) media; `grpc` and `http` provide remote clients and the
gRPC server. Every provider passes `conformance::verify`, including the
cross-account probes. Persisted formats are v1.

The `binary` feature exposes `binary::BinaryObjects` over an existing admitted
account transport. It uses the canonical Protobuf routes and the native clients'
result validators, retains each reply's original revision, and decodes body
fields from the owned response allocation. The transport owns authentication,
request cancellation and exact-key availability; it acknowledges puts durably.
Wrapping it opens no connection, and `flush` adds no request. Native providers
and futures remain thread safe; WASM providers retain browser event-loop owners.

Reads do not claim a writer revision run. A fresh `Account` over an applied
read-only medium returns the selected bytes without a local revision token;
it retains any causal minimum it cannot prove. `revisions()` reports only an
already established writer authority. A mutation or explicit `flush` opens
that authority; an embedding server retains its own applied-cut admission.

Both native remote clients accept `with_context(acyclic_auth::operation::Context)`.
Bind a view to the caller's original absolute deadline and cancellation event;
clones keep that same budget through credential resolution, connection readiness,
response reading, conversion and final admission. Use `Context::run` around an
explicit initial connection before adopting its channel with `from_channel`.
An operation-bound view uses the host's existing stop event and timer instead of
starting a relative transport timeout. An unbound view retains its configured
transport timeout.

Local expiry and cancellation retain `Error::stopped`, including cancellation
producer loss when the host maps it to cancellation. Expiry has the semantic
code `DeadlineElapsed`; cancellation has `OutcomeUnknown`. Neither stop proves
that a remote mutation did not commit. A stopped view cannot start another
credential callback; start a new operation with its own Context to proceed.
