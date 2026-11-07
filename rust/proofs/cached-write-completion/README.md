# Cached write completion custody

The public native property
`cached_flush::public_leases_keep_unfinished_flushes_until_physical_close`
keeps one original `CachedFile` lease alive through a write callback, performs
an intervening full native flush through that same lease, then performs the
callback's real write. Its `WriteAcrossFlush` case requires exactly one closing
full flush. This deterministic order is the same production transition as a
write callback paused while a second caller flushes; it claims no exhaustive
thread schedules or multi-device topology.

Marking dirty only before the callback lets the intervening flush consume its
marker before its physical write. The shared `Unflushed` guard instead marks
at callback exit, including error and unwind, before the borrowed descriptor
can be released. The same guard restores an unfinished flush's prior writes.
Only a flush after a write callback completes can retire that callback's writes.

The retained mutation replaces only `Handle::write` with its actual original
method from commit `f0e656b65641e7f46d70dd172a4015d7d5ad23f1`. It must fail
normally at
`unfinished original flush lost its physical closing flush: WriteAcrossFlush`.
A compiler error, timeout, signal, or zero selected tests is not a negative.

```sh
git apply rust/proofs/cached-write-completion/mutation.patch
cargo test --locked -p acyclic-native-runtime --test cached_flush public_leases_keep_unfinished_flushes_until_physical_close -- --exact
# Restore descriptor/cache.rs from the tested source commit, then repeat the control.
```

Work per write remains one release store, zero locks, waits, native calls,
durability barriers, allocations or copies added. The store moves from callback
entry to its stack exit. A live lease keeps the physical descriptor throughout
the callback; its guard marks before that lease can release it. Eviction can
attempt a flush concurrently, but cannot physically close this retained file.
A later full flush swaps the marker before its native operation, retaining a
write callback that completes later. Discard has its existing exclusive lease
check and flush mutex, so its earlier marker cannot be consumed concurrently.
The class census is `rg -n '(unsynced|dirty)\.(swap|store)' rust/crates/native-runtime/src/descriptor -g '*.rs'`.

The one public native control also covers flush error, unwind, successful full
completion and a write after native completion inside the flush callback. Native
attempt counts distinguish required close work from redundant close flushes.
The same-core concurrency model remains separate evidence for its mapped
close/error branches. Each native platform still requires execution; this
control does not establish ordering-only barrier or covering-device semantics.
