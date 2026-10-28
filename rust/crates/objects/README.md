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
