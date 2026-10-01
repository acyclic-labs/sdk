# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.

## Accounting evidence

`accounting::accounting_page` projects bounded, dense authority facts with exact operation identities, hash-chain digests, authoritative Stream settlement times and authenticated generation logical-byte totals. `StreamAuthorityStore::accounting_page` also reads retired authority history; retirement continues fencing ordinary reads and writes. Local and legacy records without a settlement time fail with `MissingTime`.

The caller must retain each original generation closure until its accounting projection is durably accepted under the product's protected custody boundary. This SDK reader does not implement that custody boundary or authorize garbage collection. Indefinite Stream authority history does not preserve collected generation objects. Missing historical proof returns `Proof`; it never produces a zero-byte accounting fact. Replay after collection remains incomplete until the Data/Execution owner supplies atomic proof custody or an authenticated original projection. Transport delivery ACKs do not establish this custody, application consumption, an Actor checkpoint, or billable usage.
