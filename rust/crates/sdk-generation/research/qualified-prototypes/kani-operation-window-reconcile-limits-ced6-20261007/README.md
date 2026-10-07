# Direct production Kani proof: operation-window reconciliation limits

This artifact is source-bound to the filesystem production checkout at revision `ced6f3ed8413723ba1e399855f7aff9853e79b30` and records a Kani 0.68.0 / CBMC 6.11.0 run with nightly-2026-08-21.

The proof source is `source/operation_window.rs`. It is the production `operation_window.rs` from an isolated detached proof worktree with one `#[cfg(kani)]` task-local module appended. The module imports the private production `validate_reconcile_limits` and `OperationReconcileLimits` through `super`; it does not reimplement the validator. The production file before that append has SHA-256 `441A01CC7F6211B746D243244F62FF779B37D236CB9273569A1DE77CF2896C19`; the proof source has SHA-256 `3CEC49CC46457F489953B7455C2B32BA78CD0563E0C8CAC8CAE4ABF6D6DCB645`.

The verified theorem is exactly:

> For every symbolic triple `(maximum_generations, maximum_changes, maximum_conflicts)` of `u32` values, `validate_reconcile_limits` returns `Ok(())` iff all three values are nonzero. If any value is zero, the result is `Err(WorkspaceError::JoinLimit)`.

The positive harness constructs all three symbolic fields and calls the actual private production function. It completed with `0 of 94 failed (2 unreachable)`, `VERIFICATION:- SUCCESSFUL`, one harness verified, and terminal shell exit 0. The raw terminal output is in `audit/reconcile-limits-positive-kani068.raw.txt` (SHA-256 `AAD4B70F8C10A13B0856F05CD0AA1AC513E08EF2073853B365122BE16258AF91`).

The negative control deliberately asserts that a zero `maximum_generations` value is accepted while the other two values are one. It failed at the assertion, as required: `1 of 91 failed (2 unreachable)`, `VERIFICATION:- FAILED`, terminal shell exit 0. Its raw output is in `audit/negative-control-reconcile-limits-kani068.raw.txt` (SHA-256 `A096787663E628DC63A677E0B9FC9B3088EDD25F1B55A474F9FFD4C5174D8A3D`). The nonzero shell exit is not used as the result because the command is piped through `tee`; the Kani verification status in the raw output is authoritative.

This proof covers the pure validator only. It does not prove mutation/state behavior, the four durable entrypoint call sites, async behavior, transport or wire semantics, or the separate Rust regression test.

The canonical filesystem checkout later advanced to revision c2e2d8ef80ab88a989c1f5955e6f35881f0274cd for generated bindings; its operation_window.rs SHA-256 remains 441A01CC7F6211B746D243244F62FF779B37D236CB9273569A1DE77CF2896C19, so this receipt remains bound to the same proven production validator source. The proof itself was launched from the detached ced6f3ed... snapshot recorded above.
