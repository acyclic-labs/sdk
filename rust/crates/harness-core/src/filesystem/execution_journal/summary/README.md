# Retained summary growth work

The existing generated summary property accepts production execution records,
compares retained physical allocation with the original ledger, and checks the
writer's first-insertion order. It then replays those accepted entries through
the same summary-map insertion method, isolating index copies from encoding
and pending-batch work. Insertion must copy zero prior backing.

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib filesystem::execution_journal::summary::tests::refused_tools_fund_their_summary_entries -- --exact
git apply rust/crates/harness-core/src/model/instruction_radix.patch
git apply rust/crates/harness-core/src/filesystem/execution_journal/summary/summary_relocating.patch
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib filesystem::execution_journal::summary::tests::refused_tools_fund_their_summary_entries -- --exact
git apply -R rust/crates/harness-core/src/filesystem/execution_journal/summary/summary_relocating.patch
git apply -R rust/crates/harness-core/src/model/instruction_radix.patch
```

The shared mutation restores the retired private radix only for this control;
the summary mutation restores the former production value vector. Its only
interface adaptation accepts the current iterator observer argument. Require
the runtime assertion `summary insertion copied prior backing` to fail, then
require the same property to pass after exact restoration. Compilation failure
does not establish detection. This finite property does not qualify a fuzz
campaign or larger end-to-end populations.
