# Original harness fuzz contracts

The task-volume, JSONL client and durable summary-command APIs were removed in
`8078fa93ebcc59eb039019cfc9541c38dadd646c`. Their old fuzz sources survived that
removal and no longer compile against the shipping API. The three complete
original targets are retained here as data, byte for byte. `provenance.json`
binds each source, every original corpus entry, the complete original source
tree, toolchain, manifests and resolution locks by Git identity and SHA256.
The corpus files remain at their original paths; none is rewritten or dropped.

The active `context_manifest` target keeps its original instruction, hooks,
Context, ContextSummary and ContextManifest assertions. The active
`task_volume` target keeps its original Interaction, ApprovalDecisionFile,
Stream commit conversion and persisted-observation assertions. The JSONL target
has only retired consumers, so its original registration is retained in the
captured historical manifest instead of claiming a current executable target.
No new API or corpus derivation substitutes for the historical contracts.
All retained proof source receives zero deletion credit.

Reconstruct the complete original implementation and resolution in an isolated
checkout, without copying old implementations into production:

```sh
git worktree add --detach ../sdk-retired-harness-fuzz 3b37d404357dc422e3c42da42b6e619be79fa7e1
cd ../sdk-retired-harness-fuzz
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness/fuzz-context context_manifest -- -runs=64
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/fuzz/task-volume task_volume -- -runs=64
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/fuzz/task-volume jsonl_wire -- -runs=64
```

These commands select the original source, dependency and feature boundaries;
they are reproduction instructions, not a recorded successful execution.
Current plain Cargo checks compile the live targets but do not execute fuzzing.
