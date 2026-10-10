# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.

On `wasm32-unknown-unknown`, the `browser` feature provides real IndexedDB backing.
Batch reads collect all Blob handles before awaiting materialization, and work
budgets include gate IO and the encoded replay buffers retained during decoding.
Portable provider and property tests remain compiled on this target. Only native
process, native filesystem and socket fixtures use native-only target dependencies;
the WASM test target uses portable Tokio features and the browser entropy backend.
