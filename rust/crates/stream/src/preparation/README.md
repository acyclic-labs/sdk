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
