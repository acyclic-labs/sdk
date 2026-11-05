The `native_arguments` target calls the production registry and canonical
adapters for all three shipped model profiles. It compares ordinary and native
argument admission, checks retained capacity on the original allocation ledger,
and checks retirement, exhausted credit and cancellation. The property test
`native_adapter_preserves_original_allocation_custody` uses the same checker.
Existing argument-contract tests supply independent expected mappings.

The corpus contains unchanged argument fragments from a captured provider
response and explicitly generated property inputs. `corpus/native_arguments/SOURCES.tsv`
records their origins and hashes. No external output is fabricated.

Replay from the repository root:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features native-arguments --bin native_arguments -- rust/crates/harness-fuzz/corpus/native_arguments -runs=0 -seed=1
```

Use this target and corpus with cargo-fuzz for instrumented coverage.
`drop-adaptation-ledger.patch` plants the original bug class by discarding the
caller's work before argument and metadata adaptation. The unchanged checker
must fail its retained-custody assertion. Apply the mutation only to an isolated
checkout and restore it before positive qualification. A corpus replay without
instrumentation is not a coverage measurement.
