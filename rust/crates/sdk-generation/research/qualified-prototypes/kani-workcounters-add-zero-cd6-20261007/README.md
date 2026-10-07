# Direct production proof: WorkCounters zero-add identity — 2026-10-07

This prototype proves one pure accounting property against the actual `acyclic-fs` production implementation in `filesystem-main-review/sdk`.

## Theorem

For every symbolic value of all 24 `u64` fields of production `acyclic_fs::WorkCounters`, `WorkCounters::checked_add(WorkCounters::default())` returns `Ok` and preserves the complete `WorkCounters` value field-for-field. The harness calls the exported production type and method directly; it does not copy `checked_add` or implement a second summation algorithm.

This is an identity property for adding the production zero receipt. It does not prove JSON/decimal serialization, JavaScript transport, budget admission, overflow behavior for arbitrary nonzero deltas, or filesystem operation behavior.

## Source binding

- Checkout: `C:/Users/varun/.codex/worktrees/filesystem-main-review/sdk`
- HEAD at proof: `cd6ab86bf9f5fbfa8eeb416b6604de379df67903`
- Production implementation: `rust/crates/filesystem/src/performance.rs`
- Production re-export: `rust/crates/filesystem/src/lib.rs`
- `performance.rs` SHA-256: `FF2DF7765743C19C4AE51DFA749BA3E2798AD35AEA300951FAFEB26C247ED22E`
- `lib.rs` SHA-256: `1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB`
- Harness SHA-256: `3585C094364F5685E247BB8A40CB2D9D425AA56EA77E70E3E72A760DB5A9093B`

## Run

Pinned Kani `0.68.0`, CBMC `6.11.0`, Rust nightly `2026-08-21`, CaDiCaL, offline dependencies, one build job, and task-local target directory were used. The positive run produced `0 of 457 failed`, `VERIFICATION:- SUCCESSFUL`, one verified harness, and process exit 0. Raw evidence is `audit/checked-add-zero-cd6-kani068.raw.txt`.

The separate negative control deliberately asserts that zero-add increments `object_probes`. It produced `2 of 159 failed` and `VERIFICATION:- FAILED`, as expected; this is mutation-control evidence, not a proof failure. Raw evidence is `audit/negative-control-workcounters-cd6-kani068.raw.txt`.
