# Manual Kani proofs

The opt-in harnesses call the production admission functions directly:

- `exact_u32_from_f64_all_bits` quantifies every IEEE-754 `f64` bit pattern. Acceptance is equivalent to finite, integral input in `[0, u32::MAX]`, and conversion preserves the accepted value.
- `validate_reconcile_limits_iff_all_three_nonzero` quantifies three `u32` limits. Admission succeeds exactly when all are nonzero; invalid limits return `WorkspaceError::JoinLimit`.

The harnesses compile only under `cfg(kani)`. The build script registers that cfg for stable compiler checks. Normal builds and routine CI do not run a solver.

## Reproduce

Use Kani 0.68.0, CBMC 6.11.0, Rust nightly-2026-08-21 and CaDiCaL 3.0.0 on Linux, with offline dependencies and one solver job. From the repository root:

```sh
export RUSTC_WRAPPER=
export CARGO_NET_OFFLINE=true
cargo +nightly-2026-08-21 kani --package acyclic-fs --harness kani_proofs::exact_u32_from_f64_all_bits --jobs 1
cargo +nightly-2026-08-21 kani --package acyclic-fs --harness operation_window::kani_reconcile_limits_proof::validate_reconcile_limits_iff_all_three_nonzero --jobs 1
```

Record the executed source hashes, pinned toolchain and raw terminal solver output for each new proof run.

## Evidence

The historical receipt and raw logs are preserved at commit `056625cf9ae9da8701385e98f867335d2dd9816f` on `codex/kani-proof-filesystem-c2-20261007`, under `rust/crates/sdk-generation/research/qualified-prototypes/kani-proof-integration-implementation-20261007/`. Numeric verification completed with `0 of 7 failed`; reconciliation completed with `0 of 94 failed (2 unreachable)`. Both logs report `VERIFICATION: SUCCESSFUL`.

That receipt records a dirty source snapshot containing the positive harnesses plus separate negative-control harnesses. The current positive harness bodies are identical after normalizing line endings and the final newline. Removed comments and negative controls change the complete proof-module hash. This integration does not claim a new solver run.

The production `numeric.rs` remains byte-identical to the qualified source, SHA-256 `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF`. The production `validate_reconcile_limits` function is also unchanged. The canonical numeric positive function block hashes to `E78306E119110B77AD02414C5C120A874AB1EFEB0434855EE1DBEFFE5D1CB6D8`. The current LF numeric harness file hashes to `59635972C5E04968B5875B624DD37AF74678C18E84B06E15626898D9321E9317`.

Historical negative controls deliberately asserted that unchanged production accepts `0.5` or a zero generation limit. Those assertions failed as expected. They are false-assertion controls, not mutations of the production implementation.

The proven properties concern these two admission functions and their stated pre/postconditions.
