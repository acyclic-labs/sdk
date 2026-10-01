# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.

## Hosted generation custody adapter

`AsyncObjectStore::prepare_generation_publication(request, proof, append, scope, budget, cancellation)` runs after the original closure flush and before the exact authority CAS, while `PublicationHold` remains live. It borrows the canonical `PublishGenerationRequest`, `GenerationProof`, and full `GuardedAppend` (original `ProposedCommit`, permit, epoch and expected head). The supplied closure scope includes the exact object identities and original collection sweep count. `StagedObjects` forwards this preparation to the durable provider after draining; it preserves the original collection hold through the CAS. Default preparation creates no hosted custody.

Execution owns the hosted filesystem provider and its existing accepted-operation journal. Data owns generic private Objects generation references and retained-root capabilities. The Actor restore journal and its `actors` product permission cannot authorize filesystem operations. `LogicalObjectStore` is a stateless public bucket adapter; neither its ETag nor a successful default flush is private retained-root custody.

The provider can call `kernel::encode_generation_publication_evidence` with the actual admitted `VolumeConfig` and a positive evidence byte bound. Rust validates matching request/proof/append identities and encodes the complete canonical manifest, original root, logical-file byte total, exact commit and permit. Its returned SHA256 names exactly the returned bytes, not a reconstructed history or the BLAKE3 authority identity. Runtime work counters and collection sweep counters are not durable generation facts and are excluded from the archive bytes.

Before the callback returns, the actual filesystem adapter must durably journal that original preparation and its exact `ObjectId -> GenerationRef` mapping under the protected filesystem accepted operation, then retain the original closure through Data's current private root-claim path. Accepted operation identities and exact bytes survive ambiguous outcomes and ownership transfer. Only the actual authority result can supply final acceptance, sequence/digest and settlement time; the provider must durably reconcile and bind that original outcome in its existing journal. This callback covers generation publications, not the separate initial-volume, fork-retention or pin creation boundaries, which still require their own exact accepted-operation custody integration.

`PublicationHold::none()` lets an external provider complete the existing flush contract after its own durability work. It provides no collection gate, root claim, historical proof custody or release permission. Dropping a hold must not release provider-owned durable claims. Default preparation, an accounting page watermark and delivery ACKs never authorize collection. PR #215 remains incomplete until the filesystem owner implements and supplies the atomic original-proof custody boundary for every relevant transition.

`kernel::decode_generation_publication_evidence` projects retained bytes against the SHA256 from protected original custody, with explicit byte/item bounds and canonical re-encoding. It returns original request, manifest/configuration, root, logical bytes and full append; it does not manufacture a fresh closure proof or final outcome. Execution can reuse this Rust decoder during journal recovery instead of defining a private Cloud proof serialization.
