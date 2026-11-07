# Empty ranges retain native presence without dispatch

The existing `empty_ranges_preserve_native_presence_without_dispatch` oracle
opens the real local Objects store, commits original bytes, closes it and reopens
its actual log. It counts the native runtime's original submission events while
reading valid, absent, invalid and removed empty ranges. It also checks original
index-lock contention and the public HEAD path. Nonempty reads and HEAD retain
their required native submissions; their observations check counter sensitivity.

Run `node rust/proofs/qualify.mjs objects-empty-dispatch` from the repository
root. The kept mutation disables only the production empty-range fast path.
The original worker still returns the requested empty bytes, but the unchanged
oracle must reject its extra submission at `empty range dispatched a physical
job for key`. A compile failure, empty selection or timeout cannot qualify.
The shared driver restores exact original source bytes before the positive run.

This proves zero native submissions for uncontended empty-range presence checks,
with original bytes and errors preserved. It does not prove zero locks, physical
syscalls, allocations or index comparisons, or constant cost for every Objects
operation. The contended path still uses the existing worker and completion event.
