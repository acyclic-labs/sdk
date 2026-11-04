# Retained hot-page growth work

The existing hot-page churn check retains both original preparation owners,
uses a production-encoded page, and observes copies through initial growth as
well as fixed-population churn. A frequently read page must never be evicted.

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib filesystem::execution_journal::persistent_index::cache::tests::frequently_read_page_survives_unrelated_publication_churn -- --exact

git apply rust/crates/harness-core/src/filesystem/execution_journal/persistent_index/cache_relocating.patch
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib filesystem::execution_journal::persistent_index::cache::tests::frequently_read_page_survives_unrelated_publication_churn -- --exact

git apply -R rust/crates/harness-core/src/filesystem/execution_journal/persistent_index/cache_relocating.patch
```

The mutation restores the former production radix, value vector, free-slot
vector and second-chance loop. Its only adaptation maintains the current
occupancy scalar for the unchanged bound assertions. Require the runtime
assertion `page cache insertion copied prior backing` to fail, then require
the same check to pass after exact restoration. A compiler failure does not
establish detection. Initial growth must remain covered: fixed-population
churn alone misses relocation at capacity crossings. This finite check does
not establish a fuzz campaign or all source branches.
