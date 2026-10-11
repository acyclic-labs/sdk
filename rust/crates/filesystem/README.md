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

Ordinary transactions expose `commit_with_permit_guarded` for asynchronous owner
checks and `commit_with_permit_measured_guarded` for checks returning an ordinary
work receipt. Both use the existing closure preparation and authority seal, including
stale retries that perform publication. Caller errors remain whole typed values;
failed callback work joins the SDK preparation prefix. Already-committed retries
do not perform a new CAS or demand a new owner seal. Neither route creates a native
ledger; native publication retains its distinct original-work accounting.

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
Source chunks, immutable objects and file-read payloads use `acyclic_stream::OwnedBytes`.
Native clones and slices, including empty views, keep the original allocation
custody inseparable from their bytes until final physical drop. Source packets
report actual retained buffer capacity separately from visible length; static
and opaque ordinary storage do not imply owned capacity. Native snapshot byte reads
retire request metadata before returning `OwnedBytes`; separately returned lookup
or file-reference guards cover only actual metadata, never detached byte custody.
Pending native creations keep the existing mutable batching engine. Buffer growth
and copy-on-write admit the new actual backing before replacement; a prepared
node-change permit reserves the existing ledger slot and observer work before
the held body is edited. Terminal recording commits the accepted slot even after
late cancellation, and releases the node lock before observer callbacks.
`GroupedChange::CreateFile` carries `OwnedBytes`: promotion transfers the original
body allocation guard, so restored pending and backend aliases share custody
until their last physical drop. A rejected promotion preserves the original
mutable vector and guard without resetting already reached allocation attempts.
Ordinary write/resize calls on a funded pending body borrow its same original
meter, resource allowance and cancellation token; only genuinely ordinary
unfunded bodies keep the ordinary path. The stored alias is accounting custody,
not a new namespace or task permission.
Native demand reads, source link packets and native blob-source fallback reads
prepare byte storage and carrier controls on the original meter before physical
I/O. Freezing the completed prefix allocates and copies nothing; link packet
reads share the ordinary host syscall engine without a post-read target copy.
Canonical ROOT, TREE, TABLE and ATTR encoders use the same
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
Namespace paths cache actual component-vector and name-buffer capacities, while
equality and ordering remain content-only. Owned source-chunk getters expose the
producer's existing DATA inventory, not a second allocation claim. The operation
read cache forwards publication holds, preparation and collection custody to
its underlying store using the same accounting boundary as its other callbacks.

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
FUSE drivers reserve their own known retirement credits before SDK startup:
one transient root-capability close, one session detach/join, and one invalidator
join. The wait-only cleanup path borrows those original credits at the actual
attempts; it does not replace the source's cancellation or callback authority.
Ordinary no-argument stop uses the same accepted controls without creating a
new unbounded ledger or cancellation token; it does not reset callback policy.
A cancelled cleanup wait retains unattempted handles. Consumed failed SDK
shutdowns remain failures, not absence or cleanup-complete evidence. These are
SDK-observable control attempts, not inferred transport acknowledgements or RSS.

Darwin's existing bridge retains an accepted interruption before its SDK instance
exists. The same mutex orders pre-mount stop checks and instance publication;
an already accepted stop cannot be lost and followed by an uninterruptible loop.
The private run result distinguishes a genuinely never-mounted session from a
started session that still requires physical unmount and callback retirement.
SDK loop termination uses atomic state, and constructor failure destroys its
channel instead of retaining a stopped but allocated channel. The exact compiled
bridge-session size is admitted before its real calloc; its original guard retires
only after SDK free. These guarantees do not claim opaque allocator backing or
infer namespace retirement merely from an interrupted return.

Readonly snapshot qualification uses the same mounted source checks and immutable
SDK snapshot engine. It returns the actual generation, decoding configuration and
complete source cut with no fabricated publication operation. The readonly path
rejects stale sources rather than rebinding or writing a companion. Ordinary
snapshot calls retain their existing rebinding behavior. Qualification is DATA:
callers still authenticate current Project READ before and after selecting it.
Snapshot APIs return fixed metadata only. Decoded state, backing payloads and their
original allocation guards retire together inside the capture phase; a returned
snapshot does not retain quota for already-freed state. Output containers fund
their own actual capacity before collection. Primary and reconciliation failures
remain separately typed at the mounted boundary.

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
immutable borrowed `OwnedBytes` are not adopted as new allocations.

Held native entry exchange and detached host attribution are available without
the watch feature. Windows exchange paths use the standard-library UTF-16
decoder after reserving a conservative three-byte-per-unit logical output bound.
Actual encoded length and capacity are recorded before late refusal; hidden
decoder allocation and CPU behavior are opaque, not fabricated SDK attempts.
Local path-index host attempts consume their original operation credit immediately
before the actual syscall. A physical error retains that attempted prefix and is
not replaced by a later cancellation.

Lazy publication retains its original basis, source, overlay, shadows, configuration,
candidate and permit in the same authenticated canonical DATA record. Pending and
completed companion fields are latest-operation hints, not historical authority.
Exact retries after later publications validate that original evidence and the
current owner at the actual CAS boundary. A legacy record without the evidence
retains its genuine generation and reports snapshot uncertainty; it never adopts
the current head or manufactures an absent source.

Host-private state I/O requires `NativeHostPrivateStateRoot`, opened explicitly
by trusted configured-service bootstrap under its existing creation authority.
An arbitrary path, readonly project proof or native accounting token cannot
substitute for that held directory owner. State reads, existence queries,
directory creation and writable opens use validated relative names and the same
original accounting context. Private owner publication retains existing Unix
owner/mode/link checks (and Windows regular/reparse checks), exclusive temporary
creation, no-replace held rename and prepaid temporary cleanup. It never removes
the published destination after an uncertain acknowledgement.
Directory recovery enumerates lossless native names using the same held owner,
to exhaustion or a typed original-budget failure, never an invented entry cap
or partial-success result. Only genuine `NotFound` yields an absent timestamp.

Native journal completion keeps the original protected Preparing packet.
Its durable work is a known lower bound; physical mutation before its next CAS,
or a lost CAS acknowledgement, remains an incomplete original operation rather
than a fabricated exact cold receipt. A quota reservation is accounting custody,
not permission to append after cancellation; an accepted owner window is a
separate required authorization.
Accepted action completion advances the existing packet before the next owner
guard. Its native store funds the actual record buffer and completion frame
before the physical action; uncertainty retains the original cause and live
observed prefix alongside the acknowledged durable lower bound.

`NativeAllocation::from_runtime_allocation` transfers an existing runtime
buffer guard only when its canonical allocation-owner projection matches the
exact original root and resource. It does not readmit, rebill, check cancellation
or release the payload claim. A mismatch returns the incoming guard intact;
callers must destroy the payload before either guard.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.
