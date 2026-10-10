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

## Original publication custody

`AsyncObjectStore::prepare_generation_publication` receives the original `PublishGenerationRequest`, complete `GenerationProof`, exact `GuardedAppend`, and closure/sweep scope after flush and before the canonical authority CAS. The publication hold stays live through preparation and CAS; custody failure prevents CAS. Object-store decorators forward the same context to their durable provider. Default preparation establishes no hosted custody.

`kernel::encode_generation_publication_evidence` retains the original request, root, complete sorted manifest, admitted configuration, `logical_file_bytes: u128`, proposed commit, permit, epoch, and expected head in versioned binary bytes with SHA256. Recovery requires the digest from protected original custody. Decoding returns original metadata, never a fresh proof or an accepted result. Only an exact authoritative Stream record or committed envelope establishes settlement time. A later current head cannot settle an earlier unknown reply.

Initial-volume and retention owners can encode their original proof and append with `kernel::encode_original_generation_evidence`. `AsyncObjectStore::historical_generation_evidence` is an optional provider-owned read capability: it returns original protected bytes and digest for an exact settled record. Its decoder checks the original operation, fingerprint, payload, epoch, sequence, previous digest, root, and configuration against that record. No archive, second journal, global registry, retained-root claim, or collection permission is created by these SDK values.

The external hosted filesystem provider must atomically retain original evidence and its exact `ObjectId -> Objects GenerationRef` mapping in its existing protected accepted-operation custody before CAS, retain the closure through the private retained-root capability, and reconcile the exact accepted record after ambiguous outcomes. The separate initial-volume, fork-retention, and pin boundaries need that same owner integration. The SDK does not supply that hosted service or authorize its GC. `PublicationHold::none()` is only an empty local gate, not remote custody or release permission.

## Accounting evidence

`accounting::accounting_page` takes a finite work budget and projects at most 256 dense authority facts per page. Cursors bind sequence, digest, and authoritative settlement time; each call pins a head before replay, so later concurrent appends cannot change already-consumed cursors. Facts preserve exact operation identity and canonical `Published`, `Retained`, and `WorkspaceDeleted` transitions. Logical sizes come solely from the original `GenerationProof.logical_file_bytes`, including sparse holes and counting hard-linked identities once; they do not describe RSS, physical allocation, resident memory, or billable usage.

The Stream adapter derives time from the exact authoritative record and validates committed-envelope path, sequence, and bytes. Legacy and offline commits omit settlement time and fail with `MissingTime`. A private accounting-only adapter can read retired history; ordinary reads, retries, and writes remain fenced.

Historical reads prefer the provider's protected original projection, so collected generation bodies are not needed when that capability exists. Without it, only the exact root named by the historical record is authenticated; missing or corrupt original bodies fail with `Proof`, never a zero size, invented time, or current-head substitution. Accounting cursors and transport ACKs neither establish protected custody nor authorize collection.
