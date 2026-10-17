# Prefix restore contract

ObjectsService.RestoreObjects rewinds existing bucket contents under `prefix` to `at`.
An empty prefix selects all object keys. It never resurrects the bucket itself.
This uses the same immutable generation retention and Streams log as Cloud's owning
catalog. Version IDs, physical placements and retention claims stay private.

The instant must be a positive Unix timestamp, normalized to microseconds. The
server rejects a future instant or an instant outside retained coverage with
PreconditionFailed and leaves all keys untouched. A missing bucket is NotFound.
The owner fences the verified log tip at commit: a concurrent mutation can return
PreconditionFailed/Unavailable and can be safely retried with the same identity.

A nonempty MutationIdentity is required. Reusing an identity with the identical
bucket, prefix and instant returns the original count, including after newer writes.
Reusing it with a different target is IdempotencyMismatch. The count is the number
of keys whose current value changes, not all keys under the prefix.

Authorization is the explicit `objects/restore` action on `objects:/{bucket}/{prefix}`.
A grant must cover the complete requested prefix. PUT, DELETE or LIST permissions
alone do not grant restore. The operation does not accept an account or placement
ID in its request; the existing bearer selects the account.

The SDK's in-memory/non-retaining providers return Unsupported by default; they
must never claim successful restore without retained history. Cloud implements the
server operation. The Rust gRPC/HTTP and TypeScript clients use generated messages
and the shared Rust validation through WASM.
