# Original descriptor flush completion

`FileLease::flush` gives its callback a native-owned `FileFlush`. Its private
descriptor cannot be replaced, and only its native methods can record full
completion. A successful callback without a flush retains the closing-flush
obligation. A successful ordering barrier also retains it; an actual full
fallback retires it. Error and unwind retain earlier writes, and concurrent
write completion retains its own marker independently.

The existing public property
`cached_flush::public_leases_keep_unfinished_flushes_until_physical_close`
uses ordinary durable `CachedFile` imports, real writes and real native flushes.
It counts closing attempts for all callback exits, including no native attempt,
a nested flush of another descriptor, and a requested barrier. Only native full
completion or the original device's covering drain omits its closing flush. The retained
mutation restores the former unconditional success acknowledgement at the
same production cache boundary. It must fail normally at
`unfinished original flush lost its physical closing flush: NoFlush`.
`fallback-mutation.patch` instead reports the requested policy after Linux's
actual full data flush. The same property must reject it at
`native full barrier fallback lost its completed guarantee`.

```sh
git apply rust/proofs/cached-flush-completion/mutation.patch
cargo test --locked -p acyclic-native-runtime --test cached_flush public_leases_keep_unfinished_flushes_until_physical_close -- --exact
# Restore descriptor/cache.rs from the tested commit and run the same control.
```

The standalone integration test links the deployed library, including Apple's
production device registry and native shared counters. Its existing observation
seam counts mandatory file stages, allows lawful shared device drains, and keeps
all failed and incomplete native attempts visible. It has no polling or timeout. The negative fails before any later wait. The
callback has one stack-local completion cell, no allocation or descriptor clone.
The cache retains its original mutex and atomic swap. No additional syscall,
admission, durability barrier, round trip or payload copy is introduced; only
the previously lost obligation can require its original closing flush.
Added transition cost is constant per descriptor, independent of population or
history; this is not a worst-case bound for the unchanged cache/coalescer.

Stream's original funding callback remains inside the cache lock. Its journal
completion now derives from the actual native guarantee, including full
fallbacks, rather than the requested policy. Filesystem consumers retain their
existing admission and physical data-only flush. No consumer dirty flag or
second device coalescer is added.

Census: `rg -n '(unsynced|dirty)\.(swap|store)' rust/crates/native-runtime/src/descriptor -g '*.rs'`
and `rg -n '\.flush\(' rust/crates/{stream,filesystem}/src -g '*.rs'`.
All ten public lease callback sites use the same capability. No decoder changes.

Each platform needs native execution. Apple fallback/error interception in the
existing native controls supplements real calls; it is not physical provider
failure evidence. A later shared device drain covering an earlier cached
barrier, all concurrency schedules and cross-device topology remain separate
obligations. This capability establishes only the original callback's actual
native completion.

Physical closing may also retire an earlier Apple barrier when the original
retained device's later successful drain covers its post-barrier cut. That
separate contract and its native validation limits are described in
[covering device drains](../cached-device-coverage/README.md).
