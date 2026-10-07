# Manual Kani proofs

These harnesses are opt-in and compiled only when Kani supplies cfg(kani). They
are excluded from normal builds and CI.

Pinned environment:

- Kani Rust Verifier 0.68.0
- CBMC 6.11.0
- Rust nightly-2026-08-21
- CaDiCaL 3.0.0
- Linux/WSL, offline dependencies, one solver job

Run from the repository root after installing the pinned Kani release:

    export RUSTC_WRAPPER=
    cargo +nightly-2026-08-21 kani --package acyclic-fs --harness kani_proofs::exact_u32_from_f64_all_bits --jobs 1
    cargo +nightly-2026-08-21 kani --package acyclic-fs --harness operation_window::kani_reconcile_limits_proof::validate_reconcile_limits_iff_all_three_nonzero --jobs 1

The first harness quantifies all u64 IEEE-754 bit patterns and calls the public
production exact_u32_from_f64 function. The second calls the private production
validate_reconcile_limits function from its parent module. Neither harness
duplicates the implementation.

Use the research receipts for solver output and negative controls. A successful
manual run must record the exact source revision and SHA-256 hashes of the
production modules alongside the raw Kani output.

Evidence linkage:

- The direct numeric proof receipt and raw Kani output are preserved on
  `codex/kani-proof-filesystem-c2-20261007` at commit
  `056625cf9ae9da8701385e98f867335d2dd9816f`, under
  `rust/crates/sdk-generation/research/qualified-prototypes/kani-proof-integration-implementation-20261007/`.
- Its production `numeric.rs` SHA-256 is
  `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF`.
  This follow-up keeps that production function byte-identical; the receipt
  therefore remains scoped to the same implementation, while this branch adds
  only cfg(kani) call-site harnesses and documentation.
- The reconciliation proof receipt records its production source hash and
  solver result separately; do not treat either harness as a whole-crate proof.

Validation record for this follow-up source snapshot:

- Linux ext4 source mirror, `RUSTC_WRAPPER=` and offline dependencies:
  `cargo check --manifest-path rust/crates/filesystem/Cargo.toml --package acyclic-fs --offline`
  completed with exit 0. Stable compilation emits the expected two
  `unexpected_cfg(kani)` warnings because Kani owns that cfg name.
- Pinned Kani 0.68.0 / CBMC 6.11.0 / nightly-2026-08-21 / CaDiCaL 3.0.0,
  one solver job: `exact_u32_from_f64_all_bits` completed with `0 of 7
  failed` and `VERIFICATION: SUCCESSFUL`; the reconciliation harness
  completed with `0 of 94 failed (2 unreachable)` and
  `VERIFICATION: SUCCESSFUL`.
- Snapshot hashes: `numeric.rs` =
  `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF`,
  `operation_window.rs` =
  `5A6CCB503D039679922D519C48DD69F8E213D073B3B376C97B517A9EE92559B1`,
  `lib.rs` =
  `10BC1F4E4452D065E2A68B62B48F0361229F067ACAF827DC2C8984C989803299`,
  `kani_proofs.rs` =
  `D196B61DF258FE64B01833B9FA5AF48EC845D742E21184E2198A013726FCB7F5`,
  and the reconciliation harness =
  `9EABEE12C3AC11BD5C069F1772D4983735BC8B983E8AAA9740DBBF1A81014D30`.
  The proofs call these production functions directly and assert only the
  stated function-level properties.
