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
Guarded native sync failures retain `NativeMountPublicationFailure` with the
original typed cause and `source_acknowledged`. Only real SDK source success
sets this bit; a later driver visibility failure cannot erase that observed
completion or its cumulative work prefix. `false` means unconfirmed, not absent
publication. Neither bit supplies namespace authority, a generation, or cleanup
evidence; ordinary sync projects the same original cause.
Source-constructor failure transfers any accepted watcher and its intrinsic
custody into the already-created `StartupFailed` session owner, without an
error-path source Arc or failure Box allocation. Retain that returned session
before projecting the cause; explicit bounded stop owns its actual cleanup.
`import_selected_measured_guarded` forwards an authenticated selected import
through that same mount owner without exposing its private source handle.
Both guarded methods pass the unchanged original `NativeMountMeterWork` to a
`FnMut(NativeMountMeterWork) -> Future<Output = Result<(), MountSourceError>>`
callback before each SDK seal, outside source state locks. The callback's
materialization work aliases the same original counters, allocation ledger,
resource allowance and cancellation; it does not reconstruct a bare meter,
reset the resource view, or create or merge a second receipt. Selected imports
accept only the SDK-produced opaque `SelectedImportPlan`.
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
`LazyWorkspace::mount_with_work` takes an explicit
`Option<Arc<dyn NativeMountCallbackGuard>>` after its options and before its
original meter. Task-operated factories must pass their genuine funded
owner/fence/purpose guard; the direct host-configured measured SDK projection
passes `None`. The same existing guard Arc is attached before source Arc and
watch creation and forwarded by the lazy filesystem wrapper. Meter, allocation
custody, resource allowance and source namespace never manufacture task authority.
These native measured facades return failure as `(error, WorkCounters)` inline,
including constructor denial: no counter box is allocated after a quota or
cancellation rejection. Successful calls retain their actual `OperationReceipt`.
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
zero-budget admission, dirty-snapshot rejection, original-owner guard failure,
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
