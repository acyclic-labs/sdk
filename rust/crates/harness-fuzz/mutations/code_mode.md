# Interpreter allocation negative control

The `code_mode` target calls the production interpreter with its original native
allocation owner. Its corpus contains captured tool inputs listed in
`corpus/SOURCES.tsv`; the target also checks quoted input against an independent
JSON string value and retires the real tool dispatcher.

From a clean checkout, run the target, apply the mutation, then run the same target:

```sh
cargo +nightly fuzz run code_mode --fuzz-dir rust/crates/harness-fuzz --no-default-features --features code-mode --dev --jobs 1 --target x86_64-unknown-linux-gnu -- -runs=16 -seed=1 -max_len=4096
git apply rust/crates/harness-fuzz/mutations/code_mode_detach_credit.patch
cargo +nightly fuzz run code_mode --fuzz-dir rust/crates/harness-fuzz --no-default-features --features code-mode --dev --jobs 1 --target x86_64-unknown-linux-gnu -- -runs=16 -seed=1 -max_len=4096
```

The mutation detaches the real engine from its ledger. The unchanged oracle must
reject success with zero engine credit at `assert!(result.is_err())`. A compiler
failure, timeout or unrelated crash does not qualify the negative control. Keep
the mutation out of product changes and collect coverage from the unmodified
source only.

Dispatcher retirement does not cover successful host settlement, reply decoding
or authorization. Finite fuzz runs do not establish full source branch coverage.