# Native mount qualification

The native-mount gate executes the same real-kernel behavioral matrix against
Linux FUSE, macOS loopback NFS, and Windows ProjFS. It covers projected reads,
create/write/read, nested paths, atomic replacement, rename, deletion,
concurrent handles, host watchers, orderly restoration, crash recovery, hard
links, symbolic links/reparse points, metadata persistence, platform case
behavior, bounded escape probes, and before/after fingerprints of content,
object types, link targets, and stable platform stat metadata in both the source
checkout and its Git administrative directory. Git HEAD and porcelain status
are compared independently.

The macOS loopback NFS mount does not emit FSEvents, so that backend exercises
the supported polling watcher. Linux and Windows exercise their native watcher.

Every required run qualifies a release executable and writes an
`acyclic-native-mount-qualification-v2` JSON receipt bound to its exact version
and BLAKE3 executable digest. Local runs build that executable first; the tagged
release workflow qualifies the exact downloaded Linux x64, macOS arm64, and
Windows x64 release artifacts and cannot assemble or publish until all three
backends pass. A
developer may use `--allow-unsupported` when invoking the binary directly; that
produces a successful `skipped` case with the capability probe's exact reason.
The dedicated CI scripts always pass `--require-kind`, so a missing or different
backend fails rather than skipping.

The self-hosted mount runners execute only trusted `main` pushes or explicit
manual dispatches; pull-request code is never run automatically on privileged,
persistent mount hosts.

## Measured lazy customer lifecycle

Initialize `acyclic_native_runtime::initialize_native_io_services` and
`initialize_native_mount_services` at service startup, before finite operations.
`LazyWorkspace::mount_measured` admits the real source-backed native mount;
`LazyMount::snapshot_measured` rejects dirty authored state rather than inventing
an empty lazy identity. `sync_with_permit_measured_guarded` flushes actual
callbacks and forwards the original permit and asynchronous owner guard to the
same SDK publication engine, including recovery seals. `abandon_measured`
retains the actual driver and destination-fence owner on failure for an explicit
bounded retry; failure does not trigger hidden unbounded drop cleanup.
Guarded native sync failures retain `NativeMountPublicationFailure<E>` with the
caller's unchanged typed cause, `source_acknowledged`, and actual `publication`
metadata when observed. A real DATA ACK is copied before fallible key clearing,
so finalization errors cannot erase its original operation and namespace-qualified
snapshot. A later driver visibility failure retains the actual returned source
snapshot, including an unchanged result with no DATA operation. `false` means
unconfirmed, not absent publication. These are snapshot data, never namespace
authority or cleanup evidence; ordinary sync projects the same original cause.
The same carrier's `accounting_failure` preserves a distinct actual SDK
accounting/loan-restoration error alongside the unchanged primary cause.
Qualified native source errors move both causes and any real ACK metadata into
that existing carrier; admission and driver-only failures do not invent a second
cause. The result remains inline metadata, not a fabricated heap allocation;
genuine enclosing source/callback future allocations use their actual enlarged
known Self before producer effects.
Source-constructor failure transfers any accepted watcher and its intrinsic
custody into the already-created `StartupFailed` session owner, without an
error-path source Arc or failure Box allocation. Retain that returned session
before projecting the cause; explicit bounded stop owns its actual cleanup.
`import_selected_measured_guarded` forwards an authenticated selected import
through that same mount owner without exposing its private source handle.
Native guarded sync and selected import consume an owned
`for<'m> FnMut(&'m NativeMaterializationWork) -> Future<Output = Result<(), E>>`
callback with `Clone + Send + 'static` and an owned `Send + 'static` future.
The callback receives the unchanged original materialization work before each
SDK seal, outside source state locks. Its root counters, allocation ledger,
resource allowance and cancellation are aliases, never a reconstructed bare
meter, reset resource view or merged receipt. The caller's original error `E`
is preserved inline; only genuine SDK lifecycle failures convert into `E`.
Task source publication uses the existing funded owner/fence/volume-write
callback guard, not a preparation/creation grant substitute. Selected imports
accept only the SDK-produced opaque `SelectedImportPlan`.
`NativeWorkingSet::sync_with_permit_with_work` and
`LazyMount::sync_with_permit_work_guarded` return the actual
`NativePublicationSnapshot` through their existing canonical native engines.
There is no parallel qualified/unit native facade. Authored qualification uses
the held workspace's genuine ID and volume config; lazy qualification retains
the actual source/overlay/shadow cut. Capture and head failures are unconfirmed;
source errors retain metadata only when its real ACK slot was populated.
Materialized source publication remains the final SDK operation, without a
fabricated post-publication check. Root consumers must record that exact result
before any later fallible root validation. Existing ordinary measured/unit
wrappers project unit and preserve their original error API.
`import_selected_work_guarded` consumes the actual native planner's paid
`(SelectedImportPlan, NativeAllocation)` output, with the guard immediately
after the plan argument. The guard must cover the immutable plan's actual
logical bytes on the same ledger and resource view before physical flush.
It proves only buffer custody, never source or publication authority.
The raw-plan measured wrapper retains its input once, then uses that same
producer; already-paid native plans are never adopted again. Synchronous
whole-tuple capture frees the plan before its guard even if the future is
never polled; the direct publication future preserves that drop order.
The named customer `*_with_work` snapshot, guarded sync/import, original ACK observation and cleanup
producers use the enclosing operation's meter; standalone `*_measured` calls
create one operation meter and project those same engines. Mount preparation's
head, checkout, selected-root lookup/promotion and source constructors dispatch
through their original native producers; head and checkout reuse the same full
materialization handle. Lookup output, source namespace and
source Arc payload retain actual allocation custody rather than merging receipts.
Native workspace failures preserve the typed cause before its original
allocation custody instead of discarding the error's retained buffers.
The native mount producer borrows its destination as `&Path` and admits
`copy_path` on the supplied original ledger before constructing the SDK's owned
path; a caller's existing paid path is never adopted again. The measured wrapper
uses `AsRef<Path>`, without an implicit `Into<PathBuf>` copy.
`LazyWorkspace::mount_with_work` takes an explicit
`Option<Arc<dyn NativeMountCallbackGuard>>` after its options and before its
original meter. Task-operated factories must pass their genuine funded
owner/fence/purpose guard; the direct host-configured measured SDK projection
passes `None`. The same existing guard Arc is attached before source Arc and
watch creation and forwarded by the lazy filesystem wrapper. Meter, allocation
custody, resource allowance and source namespace never manufacture task authority.
The original-work preparation, attachment and lazy mount ingresses append
`native_original: Option<&NativeMaterializationWork>` as their final argument.
This fixes the source-I/O accounting binding before source birth. Root-native
factories pass their genuine original volume work; ordinary SDK projections
pass `None`. Callback-guard presence and the source's internal SDK payer never
infer a native binding. A `None` lease retains physical custody without adopting
that SDK payer into a later caller's job meter; a native binding preserves its
original root/resource accounting constraints. Neither choice grants namespace
read or write permission.
These native measured facades return failure as `(error, WorkCounters)` inline,
including constructor denial: no counter box is allocated after a quota or
cancellation rejection. Successful calls retain their actual `OperationReceipt`.
`LazyMount::validate_snapshot_with_work` and `lookup_at_snapshot_with_work`
query the same actual immutable lazy view on the current caller's original
meter, not the source owner's callback scope. They do not flush the driver,
publish writes, expose a workspace Arc, or supply a read grant. Lookup output
retains its actual caller allocation; `NativeLazy` preserves the typed failure
before its retained custody. The original source lifetime guard remains separate.
`NativeWorkingSet::retire_with_work` accepts only a borrowed cleanup-wait token.
Its genuine capture retirement engine spends source-stored credit accepted
before birth; the caller cannot substitute a new meter or supply quota as
authority. Failure retains cleanup ownership for retry. Success returns the
opaque `NativeMountDrainProof`, retaining the original source and its intrinsic
allocation custody through an existing Arc clone, without a new allocation.
`LazyMount::retire_with_work` uses the same wait-only contract and the session's
canonical accepted-cleanup engine for driver, source, startup watcher and
destination fence. It constructs the same proof only after actual session
cleanup completion and physical source retirement, before dropping the session
owner. Errors preserve that owner for retry; no second source cancellation or
ordinary unbounded cleanup is inserted after session stop.
This proves retired view-dependent source work, not process termination or
namespace-deletion permission. Immutable generation/store and original
base-provider reads are outside that claim; a custom scratch-dependent provider
needs its own relevant accepted-reader drain rather than a tag/path-based
assumption of independence.
Both handles' `native_source_io_lease` methods borrow the actual held source and
forward its paid acceptance gate. The opaque `NativeSourceIoLease` is physical
lifetime custody only: the issuer must first authenticate the current owner,
volume and read/write path. Accepted native I/O jobs retain the lease through
physical completion; merely holding it in the outer wait future is insufficient.
Retirement rejects new leases and drains accepted ones on that same source gate.
`selected_import_snapshot_measured` performs the real native visibility barrier
and reads only the original operation's SDK ACK and retained completion
snapshot. It cannot publish fresh dirty state without an owner guard or replace
missing original completion evidence with the current workspace head.
`LazyMount::native_root_identity` forwards only the driver's authenticated
post-mount root witness, never a fresh stat or pre-mount directory identity.
`begin_original_work` returns the same real owned driver scope for retained
process namespace custody; it does not hold the session mutex during process I/O.
`with_original_work` supplies the real owned driver callback-meter scope
without holding the session mutex during kernel I/O. Its callback must pass
scope custody to the named scoped native I/O producers; each accepted I/O
payload owns a fork through physical completion, including after caller
cancellation. Future drop does not drain blocking I/O or replace that custody.
Concurrent different meters are rejected before I/O; nested
same-meter scopes share the original admission. All traffic reaching that
physical root during the scope can consume its budget. This is accounting,
not task identity: original owner, volume and path guards remain mandatory.
`LazyMount::retain_payload_allocation` transfers an already-admitted caller
container guard into the actual mounted payload without admission or allocation.
Its first guard retains the caller's independent accounting view, not mount
authority. Further absorption checks the same ledger/resource view and overflow,
leaving both guards unchanged on failure. Keep the input until its container is freed.

The canonical checkout compiler, name admission, generation mutation and safe
rebase engines accept the same original native accounting context. Copied
records move their exact paid destination-path custody; mutation batches retain
their actual vector and path guards through immutable staging. Candidate roots,
held roots and proof names retain their own primitive claims; the prepared
candidate is installed only after fallible work finishes. Authored transactions
and bulk promotion borrow guarded source content through the same streaming blob
producer. Rebase and live conflict outputs retain their original allocation
guards. Guarded direct-live resume uses the existing bounded retry controller,
unchanged publication permit and typed owner callback on that same original.
Each primitive backing is admitted before allocation; actual allocator excess
remains guarded on late denial. SDK producer bodies execute outside original-root
accounting locks, and paid mutation inputs move without release and re-adoption.
Borrowed authored slices keep their original slot witness, paths and opaque
source buffers with the caller through SDK compilation and source-ledger finish.
Only newly retained SDK paths are copied on the original context. The same
transaction loop handles both borrowed slices and genuine owned custody transfers;
the borrowed witness neither grants authority nor adopts the source graph.
Grouped changes transfer their paid slot container, while old request paths,
metadata boxes and source byte buffers remain under their real producer's
custody. Required new path copies are admitted on the original SDK ledger.
Grouped result capacity returns with its own original allocation witness.
Stable-ID and detached content, metadata/resize and named-attribute mutations
use those same original engines. Attribute names and source bytes stay borrowed
under the caller's real custody through staging; only new SDK-owned buffers are
admitted, and metadata/resize installs as one candidate update.
Generation semantic differences and exact changed-path resolution also share
their canonical engines on the full original invocation. Changed-path resolution
borrows the paid incoming delta, retains each original pinned endpoint through
the existing reverse traversal, and returns only its newly paid paths and backing
with an allocation witness. It does not replay receipts or re-adopt endpoints.
Compilation and mutation do not grant publication authority; the existing
guarded publisher still owns that boundary. The corresponding
real-source identity, concurrent-edit, denial, semantic-difference, exact-path and
orphan-prefix controls are present but have not been run for this cutover.

The ignored real-kernel customer regression
`measured_lazy_customer_preserves_mixed_identity_guard_and_cleanup_custody`
requires the native mount capability. It covers mixed authored/source reads,
zero-budget admission, borrowed destination copy denial before construction,
current-caller immutable validation and source lookup on an independent ledger,
caller cancellation without revoking the original mounted view, dirty-snapshot
rejection, original-owner guard failure,
real source success followed by cancellation of that same publication carrier
before the canonical driver verification phase, preserving acknowledged source
snapshot, cumulative prefix and retained session custody; bounded snapshot replay,
actual paid native selected imports, unpolled input
custody retirement and foreign-ledger rejection before flush, cancellation and retained
cleanup custody, including a real watcher retained after failed driver admission,
actual guarded mounted reads on the original meter, same-meter nested scopes,
pre-admitted mounted Arc payload custody from an independent caller ledger
without additional work or mount authority, cross-ledger absorption rejection
with both actual buffers' guards unchanged, and rejection of a simultaneously
active unrelated meter.
It also exercises cancelled cleanup waiting with driver/fence custody retained,
physical birth-credit retirement after revoking the original mount token, and
terminal proof replay with the original cumulative work prefix unchanged.
The real materialized working-set regression also exercises cancelled wait
retaining capture ownership, original source-lifetime revocation followed by
birth-credit retirement, and allocation-free proof custody and terminal replay.
The materialized control also retains source ownership through a cloned I/O
lease tail; both customer regressions reject lease admission after retirement.
The real selected-root source regression also checks foreign native binding
rejection before preparation/mount allocation or namespace effects, ordinary
source payer A lending lifetime to unchanged job meter B, and native source
binding rejecting B while preserving its genuine original A. Its source-byte,
directory-page and materialization counters remain zero throughout those
actual source and scope constructions.
These controls have not been run for this cutover.

Retirement proofs describe managed namespace, handle, callback and accepted-I/O
retirement. They do not equate a worker's terminal resource flag with kernel
thread exit or a join. Known SDK payload/control custody remains live through
its actual data destruction; a refused physical close stays nonterminal with
the original owner and cause retained.

## Dedicated runner prerequisites

- `linux-fuse`: Linux x86-64, the Rust release in `rust-toolchain.toml`, a readable/writable `/dev/fuse`, and
  `fusermount3`. The runner service account must be permitted to create FUSE
  mounts. Labels: `self-hosted,native-mount,linux-x64,fuse`.
- `macos-nfs`: Apple silicon macOS, the Rust release in `rust-toolchain.toml`, `/sbin/mount_nfs`, and
  permission for the service account to mount the in-process loopback NFS
  export. Labels: `self-hosted,native-mount,macos-arm64,loopback-nfs`.
- `windows-projfs`: Windows x86-64, the Rust release in `rust-toolchain.toml`, Developer Mode or an equivalent
  symbolic-link privilege, and the `Client-ProjFS` optional feature enabled.
  Labels: `self-hosted,native-mount,windows-x64,projfs`.

Run locally from the repository root:

```text
bash scripts/qualify-native-mount.sh linux-fuse
bash scripts/qualify-native-mount.sh macos-nfs
powershell -File scripts/qualify-native-mount.ps1 -Backend windows-projfs
```

## Projection fix proven by the matrix

The Windows qualification exposed that orderly `PrjStopVirtualizing` leaves a
hydrated ProjFS reparse tree at the caller's mount destination. That residue is
not an ordinary empty directory and prevents reliable reuse. `ProjFsSession`
now authenticates and removes only that stopped ProjFS cache using the existing
bounded recovery path, recreates the caller's empty destination, and retains
cleanup state when cleanup fails so an exact stop retry can finish it. No
checkout, publication, or distributed-filesystem semantics changed.

The Windows metadata case also proved that ProjFS reports a metadata-only
`FileBasicInfo` change as a handle close without data modification. That close
previously skipped host capture when the path already existed, discarding such
changes. The callback now records per-open basic-metadata baselines and captures
only representable changes. It ignores ProjFS's expected placeholder-hydration
attribute transition and automatic access-time updates, so ordinary reads stay
side-effect free while the existing bounded capture transaction preserves
authored attributes and timestamps.

The macOS crash case also proved that comparing `st_dev` with the destination's
parent is not a valid NFS mount-presence test: the loopback export can report the
same device identity while still mounted. Recovery now checks Darwin's mount
table by exact mount-point name before removing the destination, preventing the
observed `EBUSY` race without changing NFS request handling. The crash test
allows one exact recovery retry because a forced detach of a dead loopback NFS
server may finish asynchronously after the bounded unmount helper returns.
