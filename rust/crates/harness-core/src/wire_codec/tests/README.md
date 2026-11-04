# Borrowed index custody

The production history adapter retains every shared index node on its original
observer until the node backing retires. The test uses standalone and native
preparation observers, physical allocation accounting, constructor refusal,
exact byte admission, cancellation and retirement after cancellation.

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib wire_codec::admission_tests::borrowed_index_nodes_keep_original_credit_on_refusal_and_cancelled_retirement -- --exact
```

The retained mutation bypasses byte retention in the actual adapter:

```sh
git apply rust/crates/harness-core/src/wire_codec/tests/borrowed_index_unfunded.patch
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib wire_codec::admission_tests::borrowed_index_nodes_keep_original_credit_on_refusal_and_cancelled_retirement -- --exact
git apply -R rust/crates/harness-core/src/wire_codec/tests/borrowed_index_unfunded.patch
```

Require the runtime assertion `borrowed index backing must retain its original
credit`; compilation failure is not detection. Restore the exact source and
require the same test to pass. Source-selection semantics remain covered by
`model::instruction_sources::tests::shared_sources_preserve_bytes_errors_scope_and_first_occurrence`
and `model::instruction_sources::tests::discovery_and_projection_preserve_source_work_and_original_custody`.
The shared collection properties cover bounded index traversal; these adapter
checks cover original custody rather than restating the radix algorithm.
