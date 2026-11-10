# Settled mutation selection

`applied` borrows the canonical request and its original settled skipped-path
subsequence. Each soft append compares only with the next skipped path. The
cursor advances on equality, so traversal does one path comparison per input
soft append and O(M + B) work for M mutations and B examined path bytes, with
no allocation or path copy. Cursor bookkeeping is O(1) per input mutation.
One iterator `next` may consume several skipped mutations before returning.
Cloning an iterator preserves its partially consumed source and skip cursors.

The retained `settling_copies_only_admitted_skipped_paths_and_views_the_rest_in_place`
property exercises the production traversal with canonical mixed append, fork
and soft-append requests. It checks original reference identity, a cloned
partially consumed iterator, lazy creation, exhausted iteration and the actual
path-comparison bound. Its comparison observer is the same generic traversal
used by the public entry point, with ordinary path ordering in production.

```sh
cargo test --locked -p acyclic-stream --no-default-features --lib preparation::tests::settling_copies_only_admitted_skipped_paths_and_views_the_rest_in_place -- --exact
git apply rust/crates/stream/src/preparation/repeated_search.patch
cargo test --locked -p acyclic-stream --no-default-features --lib preparation::tests::settling_copies_only_admitted_skipped_paths_and_views_the_rest_in_place -- --exact
git apply -R rust/crates/stream/src/preparation/repeated_search.patch
```

The planted mutation restores the production binary search. Require the runtime
assertion `selection searched settled history again`; a compilation failure is
not detection. Restore the exact source and require the same property to pass.

## Canonical shape membership

Normalization sorts conditions and mutations and rejects duplicate paths before
shape validation. Condition selection advances one cursor over that original
condition slice. Each comparison consumes a condition or finishes one mutation,
so there are at most C + M comparisons for C conditions and M mutations.
The first soft append that needs a carrier checks the original mutation slice;
subsequent soft appends reuse that result. Requests without such a soft append
do no carrier scan. Both operations allocate and copy nothing, and add no wait.
Fork-descendant overlap validation is unchanged and has a separate cost.

The same property now covers 768 canonical mixed and soft-only-prefix requests,
with carriers both before and after soft appends and unmatched conditions on
both sides of mutation paths. The production shape validator's observer counts
condition comparisons and carrier examinations independently. The public entry
point runs that same implementation with a zero-sized observer.

The retained `repeated_shape_search.patch` restores both repeated walks in the
production validator. Apply it, run the same exact property command above, and
require `shape validation repeated scans: carrier and condition excess` with
nonzero excess in both fields. Restore with `git apply -R` and require the same
property to pass. Compilation failures are not detection.
