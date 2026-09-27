# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation or deletion returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/stream) are the sources for exact limits and commit behavior.
