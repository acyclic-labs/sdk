# Instruction read projection

Read projection uses the shared fixed-key index for current sources and pending
calls. Every result consumes its pending call, including partial and non-read
results. Complete call identities, source identities and content authentication
remain required after an indexed lookup. Nodes borrow the original observer
until retirement; no prior node backing relocates as the history grows.

The retained projection property compares complete output bytes and authentication
traces with an independent baseline. It checks original custody retirement and
equal original work/allocation deltas for equal history increments. The generated
private-index property remains while other journal consumers use that index.

The retained production mutation restores a prefix visit before each result:

```sh
git apply rust/crates/harness-core/src/model/instruction_reads_prefix_scan.patch
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib model::instruction_review_tests::read_projection_has_exact_linear_index_work_and_quadratic_negative -- --exact
git apply -R rust/crates/harness-core/src/model/instruction_reads_prefix_scan.patch
```

Require the runtime assertion `history traversal must visit only the presence
prefix and each message/part once`, then restore the exact source and require
the property to pass. A compiler failure is not detection. This tests the real
production traversal; it does not replay a separate handwritten radix model.
