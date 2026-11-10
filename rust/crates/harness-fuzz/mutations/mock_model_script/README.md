# Request-line binding

The existing `mock_model_script` target calls the production `bind_result`
entry point. Its retained corpus includes a single matching line, an ambiguous
pair and data containing literal template syntax. The oracle requires exactly
one selected line and preserves the selected bytes without further expansion.

Run from the repository root on Linux x86_64, selecting the target explicitly
to keep instrumentation out of build scripts and proc macros:

```sh
RUSTFLAGS="${RUSTFLAGS:-} -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares" cargo run --locked --target x86_64-unknown-linux-gnu -p acyclic-harness-fuzz --no-default-features --bin mock_model_script -- rust/crates/harness-fuzz/corpus/mock_model_script -runs=0 -seed=1
```

Apply `accept-ambiguous-line.patch` and run that same command and unchanged
corpus. The ambiguous input must fail the selected-result cardinality assertion;
a build or setup failure is not a detected defect. Reverse the patch before
running the positive control. This qualifies request binding, independently of
the installed native hosts' workspace and sandbox contracts.
