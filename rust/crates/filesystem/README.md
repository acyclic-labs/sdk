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
Standalone native measured wrappers return failures as an inline `(cause,
WorkCounters)` pair. Constructor rejection does not allocate a counter box.
Composition uses borrowed `*_with_work` / `*_with_native_work` producers on the
same original ledger, rather than creating a remaining-budget ledger and merging
receipts afterwards.
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
Dropping an observer does not drain accepted work: its mounted original-work
activation and payload custody remain in the accepted worker until completion.

The Memory immutable-object store uses one sorted resident vector and binary
search for both ordinary and native operations. Capacity is admitted before
reservation; observed capacity remains guarded until its backing buffer dies.
Replacement frees old backing before releasing its guard. Native encoded objects
move their existing vector custody into the immutable `Bytes` owner, so persisted
aliases retain it until their final physical drop without adopting the data twice.
These are logical SDK-owned payload/slot bounds, not allocator/RSS or opaque
standard-library tree-node footprint claims.

Concurrent checkout observers share the semantic proof, not allocation deltas.
Each observer retains exact name/path DATA and known control claims; cancellation
and unwinding preserve its own completed prefix. Copy-on-write claims precede
copies, weak-only moves preserve DATA custody, and snapshots distinguish reused
addresses using checked allocation generations. Custody metadata does not change
semantic equality.
Native proof claims carry the resource attribution of their actual paid guards
until physical retirement. Observer and candidate-diff factories use the same
canonical engines on the original carrier; guarded diff output includes both
vector capacities and every owned key/before/after name buffer. Cached proof
snapshots inventory DATA without adopting it again.

Native stock reads use the original allocation/work carrier through Memory,
Logical Objects, cache, staged storage and simulation. Logical Objects reserves
bounded logical returned-payload quota before provider I/O, then reconciles the
visible payload length. Opaque provider allocation/backing capacity is not an SDK
allocator/RSS claim. Existing shared Memory/cache bytes are borrowed without
readoption; new SDK vectors, request strings and copies are admitted separately.

`NativeMaterializationWork::read_owned_bytes` reads an already-authorized reader
through the original operation and byte reservations; a one-byte probe detects
oversized input. `copy_bytes` uses the same paid byte-buffer engine. Both return
the actual vector before its original capacity guard, and preserve completed
read/copy prefixes on denial. Neither opens a path nor grants content authority.
`write_owned_bytes` accepts an already-authorized writer and borrowed input. Each
actual SDK write attempt and completed partial output is recorded before exposing
late cancellation, quota exhaustion or the original I/O failure. It makes no
input copy or implicit flush and does not grant write permission or durability.
`shares_allocation_context` compares only the original ledger and exact resource
attribution; pure identity checks are not authority or new budget admission.
`reserve_work` withholds additive quota on the same resource without reporting
spent work. It excludes allocation/live-byte dimensions and is not permission to
append or mutate after cancellation; an accepted completion window supplies that
separate authority. Only real completion consumes credit; unused credit refunds.

Mounted callback receipts include SDK-completed prefixes from ordinary lookup
errors as well as successful callbacks. Expected misses do not poison the source.
An owned first fatal callback diagnostic is taken once without resetting the
original cancellation, allowance or reconciliation requirement. Driver reply APIs
that expose no transport result report client delivery as unknown: source/backend
read and write counters are not IPC reply acknowledgements. Real mounted-client
bytes and termination remain the qualification oracle.

Installed native sources retain their exact original accounting context, including
resource-less contexts; callbacks do not reset that operation's allowance. This
header is not task authority. Task-backed mounts install an authentic async owner
guard, polled on the existing callback runtime before provider exchange allocation
or effects; direct host-configured mounts retain their explicit host role.
Authorization failure moves its typed cause into a preadmitted per-callback box,
not a shared diagnostic slot. The same box is unsized without another allocation
and physically destroyed before its outer allocation custody is released.
Directory pages are non-cloneable intrinsic owners of entry/name/cursor capacity.
Physical projections transfer and split their existing custody for escaping data,
without adopting that data again or creating another ledger.

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
Immediate Authority preserves its existing HashMap lookup/idempotency engine.
Its finite allowance covers SDK-known typed entries and owned strings/record
buffers; standard-library bucket layout, allocator overhead and RSS are opaque
and excluded, as with BTree nodes. A real growth reservation counts an SDK
allocation attempt; public map capacity is not a physical bucket-byte witness.

Known `Arc<Self>` metadata and owned payloads are admitted before the SDK
allocation attempt and charged until `Self` is destroyed. The last intrinsic
guard follows owned vectors, strings, queues and handles; opaque standard-library
Arc backing retained by `Weak` after `Self` destruction is excluded. This is not
a physical allocator-block, parser-heap or RSS guarantee. Private serde codec
error backing is similarly opaque; SDK-created diagnostic paths and strings
still retain their original paid custody.

Original native accounting holds its shared lock only for admission, completed
counters and custody bookkeeping. Allocation, serialization, physical I/O and
observer callbacks run outside that lock. Native transaction frontdoors feed
the same canonical authored-mutation compiler with the actual input custody;
immutable borrowed `Bytes` are not adopted as new allocations.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.
