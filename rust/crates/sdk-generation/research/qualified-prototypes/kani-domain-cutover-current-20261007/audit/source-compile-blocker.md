# Current source compile blocker

Both direct production commands were run sequentially from the current
Q checkout with Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, offline,
one build job, and a task-local Linux-ext4 target.

The commands reached acyclic-actors compilation and exited 1 before any Kani
harness or CBMC solver started. Rust emitted E0583 at
rust/crates/actors/src/domain.rs:17 for mod kani_proofs. Because domain.rs is
included inside contract::generated::domain, Rust resolves that child module
relative to the actors source directory and requests
rust/crates/actors/src/kani_proofs.rs. The current checkout contains only
rust/crates/actors/src/domain/kani_proofs.rs.

This is an actual current-source layout failure. No copied module, path shim,
production edit, or proof-only replacement was introduced. A future run
requires the source owner to resolve the module layout and then a refreshed
source inventory; the current receipts remain compile-blocked and make no
theorem claim.
