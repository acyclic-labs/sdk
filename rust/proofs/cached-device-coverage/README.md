# Cached ordered writes and covering device drains

The physical descriptor's existing dirty marker also holds an ordered device
cut. Zero means clean; `u64::MAX` means writes require their own flush. An
ordered cut is sampled after a successful native barrier, using only an already
resolved `HeldDevice`. Closing can omit I/O only when that same device's
successful completion covers the cut. Every later write replaces it with the
unconditional dirty marker. Restoring a cut uses compare-exchange so a write
that completes inside the flush callback cannot be overwritten.

The public `FileFlush` records the actual native guarantee. No caller supplies
a device identity or completion number. The original physical handle owns its
device evidence, and reopening creates fresh evidence. Unresolved, nonlocal or
unrepresentable cuts retain the original full closing flush. Counter exhaustion
cannot create a clean or unconditional-dirty sentinel as an ordered cut. At
exhaustion the original coalescer and shared adapter retain each request's own
drain; they never wrap or reuse the last number as a new covering ticket. The
existing drain-retirement property and its retained counter negatives cover
that boundary; native Apple mapped execution remains required.

Bug class: confusing an ordering operation with durable completion, or retaining
an unnecessary flush after original device completion already covers it. Census:
`rg -n 'unsynced|impl Files|FileFlush|HeldDevice' rust/crates/native-runtime/src/descriptor rust/crates/native-runtime/src/file rust/crates/native-runtime/src/device_flush.rs`.
The single production cache and the existing native device counters own all
transitions. There is no consumer dirty flag, new registry or second coalescer.

The existing real public lease property now includes an ordered original file
followed by another file's drain, a write after that drain, and a write completed
inside the barrier callback. It uses ordinary durable imports and native writes
and flushes. Linux and Windows exercise their actual full-flush fallback;
Apple exercises ordered cuts only where its barrier is supported. The existing
real file-lock/coalescer property checks that failed/unwound drains cover no cut,
that a later successful drain covers an earlier cut, and that a drain already
running when a cut is sampled cannot cover it.

Retained production negatives:

- `early-cut.patch` samples the current drain rather than the next one. The
  original coalescer property must fail at `failed original drain covered an ordered cut`.
- `no-coverage.patch` drops the ordered cut. On Apple the public property must
  reject the extra closing flush for `CoveredBarrier`.
- `local-coverage.patch` reads only the process-local completion counters.
  On Apple the public property must reject the extra closing flush for
  `ProcessCoveredBarrier`, after another real process completes its full flush.
- `entry-evidence.patch` moves the real flush state from the physical handle to
  the surviving entry, sharing it across reopen. On Apple the public property
  must reject the missing closing flush for `ReopenedBarrier`. The mutation
  changes the actual ownership boundary; it does not replace native I/O.
- `stale-cut.patch` restores a cut unconditionally. On Apple the public property
  must reject the missing closing flush for `WriteAcrossBarrier`.

```sh
git apply rust/proofs/cached-device-coverage/early-cut.patch
cargo test --locked -p acyclic-native-runtime --lib device_flush::tests::failed_and_unwound_drains_release_original_lock_without_acknowledging -- --exact
# Restore device_flush.rs from the tested source and repeat the same control.
```

The other four negatives use
`cached_flush::public_leases_keep_unfinished_flushes_until_physical_close`.
A compiler failure, timeout or no selected tests is not a negative result.
Native macOS execution is required for those four controls. This standalone
integration property links the library without `cfg(test)`, so Apple uses its
production global registry and native mapped shared counters. It observes each
mandatory file stage while allowing lawful shared device drains; failures and
incomplete attempts remain visible. Apple unit-only interception/classification
controls still use their separate test registry and cannot qualify that deployed
path. Multiple devices, producer failures during a native drain and every
concurrent schedule remain obligations.

The same property starts a fresh native child process with the public cache
configuration set to one idle descriptor. Two real files force eviction while
retaining the original `CachedFile`; `lease_charged` must record exactly one
`DescriptorCost::Reopen`. Its new physical handle performs a barrier followed
by a different file's full flush. An unresolved new owner retains its closing
flush, even though its preceding physical handle resolved that device. The
child exits through the original process completion event; there is no scan of
host capacity or readiness polling. Linux and Windows use their actual full
barrier fallback, so only native Apple execution can qualify the retained
entry-evidence negative.

`ProcessCoveredBarrier` primes and stages the original physical handle, then
runs a full flush through another process's public cached lease on the same
filesystem. That child gets its own `TMPDIR`, uses the deployed device registry
and acknowledges only after its native full flush succeeds. The parent checks
both process success and that completion marker, so a missing child test cannot
pass. Closing the original file must add no mandatory file stage. Parent stage
counts exclude the child's calls; its actual returned native result supplies
completion. The existing helper owns all child invocations and waits through
`process_output`; no private counter file, fake drain callback or OS fixture is
introduced. Linux and Windows qualify their native full-barrier fallback, not
Apple shared-counter coverage. The latter and its retained local-counter
mutation require native macOS execution.

Required work: a write still performs exactly one release store, with the
existing atomic widened from one byte to eight; allocation funding continues to
use the actual handle size. An ordered callback replaces its old restoration
store with one compare-exchange. After staging, a resolved device samples one
existing shared counter, or locks its existing local counter state once if
shared setup failed. Closing checks one completion counter in the same domain.
A covered Apple close changes from one `fsync` plus one device-drain attempt to
zero native calls. No allocation, descriptor clone, admission, payload copy,
round trip or wait is added. All new state transitions are constant work per
physical descriptor, independent of population/history; the unchanged device
map and cache eviction are not proved worst-case constant time.

Area cleanup: full, ordered and unchanged results replace the private Boolean
completion protocol. Ordered evidence reuses the existing dirty marker rather
than introducing another owner, flag or counter. Existing unwind, unconditional
acknowledgement and write-entry negatives are adapted to that same protocol.
No decoder or persisted format is changed.
