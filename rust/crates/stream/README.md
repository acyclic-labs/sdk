# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

The `http` feature provides `http::HttpStream`, implementing the same provider interface over hosted JSON routes. It accepts HTTPS (or loopback HTTP for local tests), a bearer token, a response byte bound, and an optional private CA. Follow polls bounded read pages; dropping the stream cancels polling. Mutations are not automatically retried. Successful Commit and committed idempotency inspection fetch `commits/read` to return the full Rust envelope, so those credentials need both mutation and commit-read access.

Replicated providers can use `request::append_digest`, `request::fork_digest`, and `request::commit_digest` for the canonical state-independent validation and retry digest. Commit normalization sorts participants and rejects duplicate paths; providers still own atomic authorization, state checks, admission, durability and deadline decisions.

`preparation::append`, `preparation::fork`, and `preparation::commit` construct canonical outcomes and immutable envelopes from pre-commit existence/tails, an accepted commit ID and commit timestamp. Coordinated preparation requires explicit observations for every participant and fork source; missing observations are invalid. Forks always use the source's pre-commit prefix, including when that commit also appends to the source. The memory provider shares these condition, authority and record construction helpers. Providers must perform retry lookup first and obtain observations, authorize, reserve capacity and publish atomically; preparation itself performs no durable acceptance or retention.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

`LocalStream` uses a V3 journal and snapshot format. Earlier V2 local stores are unsupported because a store that used trim or delete cannot meet the full-history guarantee. Opening a recognized V2 journal or snapshot returns `LocalStreamError::UnsupportedFormat` without rewriting it. There is no in-place migration; use a new local root.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/stream) are the sources for exact limits and commit behavior.
