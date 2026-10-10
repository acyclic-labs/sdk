# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

Durable retry intents must retain their original immutable generation, operation key and exact mutations before dispatch. Embedded `Workspace::begin_transaction_at` and hosted `HostedWorkspace::begin_transaction_at` reopen that exact owned base. After a lost acknowledgement, reload the original generation with `generation(...)`, reuse the key and replay the same edits; beginning at a fresh head is a new transaction, not a retry. Foreign workspace/client generations are rejected, and canonical idempotency, conflicts and fencing remain enforced.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

With `distributed`, compose `Fs::new(StreamAuthorityStore::new(streams), RemoteLogicalObjectStore::new(objects, bucket), capabilities)` for authenticated remote Streams and Objects. `objects` is an `Arc<P>` where `P: acyclic_objects::v1::ObjectsProvider` (including `GrpcObjects`); `bucket` is the account's exact dedicated `wire::BucketRef`. Authentication, account routing, bucket provisioning, and durable acknowledgement belong to those providers, not the filesystem adapter.

`RemoteLogicalObjectStore` keeps no local authority or durable object state. It validates canonical content digests, uses immutable conditional publication, verifies existing content on conflicts, and charges every individual PUT/GET. Its write groups are not atomic: a failed group can leave an unreferenced durable prefix, but authority publication occurs only after the required objects are durable. Native memory/local compositions continue to use `LogicalObjectStore<P: NativeBatchObjects>` for genuine one-operation batches. Reopening a remote filesystem must use the same authorized Streams namespace and Objects bucket; external deletion/collection must not remove content reachable from published generations.

Remote write groups admit one compact index allocation under the work budget, validate every input before issuing PUTs, and publish each distinct object in first-occurrence order. Duplicate content does not add a PUT; retrying an already published object verifies its retained bytes and charges the readback.

Shared service-account deployments must use `Fs::new_in_namespace` with a stable
namespace derived exclusively from the verified tenant and deployment realm,
and `RemoteLogicalObjectStore::with_key_prefix` with the same tenant scope.
The namespace binds every workspace reference; the prefix isolates even equal
content digests in the backing bucket. Neither scope may come from public RPC
metadata or workspace selectors. Recreate these stateless engines on any
replica; only Streams decides publication, fencing and operation replay.

Workspace names identify a single lifetime within a deployment namespace. Deletion
terminally retires that identity: a fresh open returns typed `NotFound`, and the
name cannot create a replacement lifetime. Retrying deletion with the original
reference returns `AlreadyCommitted`; a mismatched reference is rejected before
storage access, including after retirement. Use a new name for a new workspace.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.
