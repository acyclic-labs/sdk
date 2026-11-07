# Cached flush unwind custody

`cached_flush::public_leases_keep_unfinished_flushes_until_physical_close`
opens real files through public `CachedFile::new` and `CachedFile::lease` with
ordinary durable configuration. It writes through the original lease, then
counts the actual native durability attempts from the existing observation
seam. Error, unwind, successful full flush, a later write during the flush
callback, and a write callback spanning a full flush share one control. The
write cases order real writes after native completion; they do not claim
exhaustive concurrency schedules.

A successful full flush needs no closing flush. Error or unwind before physical
completion retains exactly one closing flush; a write made after a completed
flush also retains one. The standalone integration test observes the deployed
library, counting each mandatory file stage while allowing an Apple device
drain to be shared by another process. Every failed or incomplete attempt
remains visible. It waits on no timer or readiness loop.

The retained mutation transplants the original error-only restoration from
`Handle::flush` in commit `e0e499d2d6ea6def6682d0f31517f7f54b372673`,
adapting only the completion tuple and retaining ordering-only custody on normal
return. Unwind still skips restoration. The public control must fail normally at
`unfinished original flush lost its physical closing flush: Unwind`.
A timeout, compiler error, signal or zero selected tests is not evidence.

```sh
git apply rust/proofs/cached-flush-unwind/mutation.patch
cargo test --locked -p acyclic-native-runtime --test cached_flush public_leases_keep_unfinished_flushes_until_physical_close -- --exact
# Restore descriptor/cache.rs from the tested source commit, then run the same control.
```

The production change replaces the error-only restoration closure with the
same scoped guard that retains a write callback's dirty marker on exit. The guard restores before the existing flush
mutex is released, including during unwind. Successful completion disarms only
that restoration; it cannot clear a later write's marker. No public callback,
new owner, mutex, allocation, descriptor, native call or durability barrier is
added. The existing same-core model remains responsible for concurrent
close/error ordering; this control establishes the mapped public native paths.
Native execution on each platform is required before claiming that platform.
Ordering-only barrier custody and covering device-flush accounting are separate
obligations that this change does not qualify.
