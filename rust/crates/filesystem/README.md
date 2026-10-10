# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Native materialization distinguishes an immutable `GenerationTransition { from, to }`
from `NativePreparation`: preparation retains the actual qualified source selection
and original full manifest/fence bytes rather than inventing a generation transition.
Preparation, inspection and cleanup share the existing materialization journal.
Typed root and preparation observations do not manufacture verified evidence for a
raw backend. Ordinary `MaterializeOptions` use `expected_root_identity: None`;
host-bound preparation supplies the actual witnessed identity and rejects replacement.
Original creation authority, current caller grants and generation conflict checks
remain separate requirements. Failure receipts retain completed work, including work
performed before a cancellation or budget failure; measured helpers use the canonical
allocation ledger rather than fixed counters.
Native Harness preparation selects fully finite resource allowances; the low-level
SDK still honors intentional `UNBOUNDED` allowances in the same measured engine.
Native host root and directory witnesses retain raw 16-byte allocation identities
across held-handle
mutations. Unsupported platform mutations report a typed error; recursive cleanup
checks original identities instead of treating a path as ownership. Cold namespace
discovery uses bounded breadth-first traversal, while warm caches retain reverse
aliases. Memory and local lazy providers use their actual stores and canonical
treap state. A checkpoint may report cancellation or budget failure after durable
CAS; reconciliation retains all measured work and resumes the same WAL, not a
replacement journal.

Initialize `acyclic_native_runtime::initialize_native_io_services()` and, when
mounting, `acyclic_fs::initialize_native_mount_services()` during explicit startup,
before supplying producer budgets. These initialize the existing native and callback
services; measured calls never create a hidden worker pool. Cold measured calls
report an unavailable service, and startup errors retain their original OS cause.
An absolute native source constructor reports three physical backend reads:
opening the root, obtaining its held identity, and discovering locality. A
two-read allowance denies locality discovery after retaining the two completed
reads; SDK-stage counting must not hide the identity query.

Native demand pages retain immutable entries and the original cursor until
`SourcePagePublication::accept` succeeds. Consumers reserve the publication's
reported cleanup work before projecting entries; abort or drop restores the original
cursor, including an unpublished first page. Publication is not authority.
Owned native blob chunks report actual retained buffer capacity separately from
visible byte length. Canonical ROOT, TREE, TABLE and ATTR encoders use the same
engine for ordinary and measured calls, admitting encoding allocations and work
before construction. Necessary short-frame future boxes likewise admit their actual
compiled size and preserve nested failure work, rather than using a fixed estimate
or removing the stack boundary. Cancellation remains cooperative with OS I/O;
receipts wait for accepted workers and their retained buffers to retire.

Mounted callback receipts include SDK-completed prefixes from ordinary lookup
errors as well as successful callbacks. Expected misses do not poison the source.
An owned first fatal callback diagnostic is taken once without resetting the
original cancellation, allowance or reconciliation requirement. Driver reply APIs
that expose no transport result report client delivery as unknown: source/backend
read and write counters are not IPC reply acknowledgements. Real mounted-client
bytes and termination remain the qualification oracle.

Concurrent native read observers share the checkout's semantic proof but admit
and report their own completed name/path DATA. Weak custody metadata follows
the actual owned keys, including weak-only map moves, and retires when those
keys die; inspecting completed custody neither allocates nor checks cancellation.
Actual COW name/path buffers are admitted before copying. The same canonical
short-frame helper records exact compiled frame ownership in each observer's
pending work, including unwind, without treating frames as retained proof DATA.
Readonly list vectors and sealed candidate proof preparation are admitted before
construction; adopting an already prepared candidate does not allocate.
Inline tree control metadata and opaque BTree layout are not inferred as DATA.

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
