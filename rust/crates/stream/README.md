# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

`MemoryStream` and `LocalStream` retain retry outcomes and immutable commit envelopes indefinitely within their explicit capacity limits. Replay precedes capacity admission; a new identity fails closed when the store is full. An admitted identity cannot execute again or acquire another digest after a clock advance. `LocalStream` preserves these outcomes through journal replay and snapshots, including existing snapshot expiry metadata. Outcomes discarded by older implementations cannot be reconstructed from a snapshot that no longer contains them. Hosted services must provide the same no-reexecution guarantee under their own durable account-wide retry authority; local checks do not qualify that service behavior.

The `http` feature provides `http::HttpStream`, implementing the same provider interface over hosted JSON routes. It accepts HTTPS (or loopback HTTP for local tests), a bearer token, a response byte bound, and an optional private CA. Follow polls bounded read pages; dropping the stream cancels polling. Mutations are not automatically retried. Canonical successful Commit and committed retry observations include the full immutable `envelope`, so mutation-only credentials need no follow-up commit read. Published compact-only servers still require `commits/read` access for those operations.

Native servers can use `http_response::encode(route, protobuf_bytes, maximum_json_bytes)` to project generated append, fork, Commit, commit-read, child-page and retry-observation responses through the same Rust code used by WASM. The successful Commit retains its existing `ok`, `commitId`, `tails` and `forks` fields and adds the complete `envelope`. The encoder does not authorize or durably accept mutations.

Recovery admits surviving snapshot receipts plus at most one outcome/envelope per bounded journal command before restoring configured live receipt/commit limits. This preserves old post-expiry journal commands without evicting surviving outcomes; fresh live mutations fail closed when recovered inventory exceeds capacity.

Replicated providers can use `request::append_digest`, `request::fork_digest`, and `request::commit_digest` for the canonical state-independent validation and retry digest. Commit normalization sorts participants and rejects duplicate paths; providers still own atomic authorization, state checks, admission, durability and deadline decisions.

`preparation::append`, `preparation::fork`, and `preparation::commit` construct canonical outcomes and immutable envelopes from pre-commit existence/tails, an accepted commit ID and commit timestamp. Coordinated preparation requires explicit observations for every participant and fork source; missing observations are invalid. Forks always use the source's pre-commit prefix, including when that commit also appends to the source. The memory provider shares these condition, authority and record construction helpers. Providers must perform retry lookup first and obtain observations, authorize, reserve capacity and publish atomically; preparation itself performs no durable acceptance or retention.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

`LocalStream` uses the existing V4 journal and snapshot format. Recovery validates headers, configured limits and checksums and fails closed on incompatible or corrupt durable data. These changes do not rewrite the disk format or remove history.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/stream) are the sources for exact limits and commit behavior.
