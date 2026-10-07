# Direct-production Actors Kani proof integration (d690)

This isolated branch qualifies the canonical d690 Actors semantic source. The
existing production domain module already binds domain/kani_proofs.rs under
cfg(kani); this branch adds one cfg(kani)-only negative-control module and keeps
all theorem calls against production types and conversions.

Source freeze:

- Git revision: d69057b2b248fcf930aaf962bbaadd0d52297731
- Worktree: C:\Users\varun\.codex\tmp\kani-proof-actors-d690-20261007
- Toolchain: Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, CaDiCaL 3.0.0
- Positive and negative runs used one harness and RUSTC_WRAPPER disabled after
  the first enum attempt exposed an sccache output-path failure on the WSL
  /mnt/c mount.

Production proof bindings:

- positive_u64_constructor_accepts_exactly_nonzero_values calls the production
  PositiveU64::new and checks all symbolic u64 values, including exact u64
  preservation and the production invalid-argument error.
- enum_numeric_mappings_are_inverse_and_lossless calls the production
  SubscriptionState, ActorState, and ErrorCode TryFrom mappings plus the
  production numeric conversions for arbitrary symbolic i32 values.
- kani_negative_controls.rs contains intentionally false claims only:
  PositiveU64 accepts zero, and SubscriptionState accepts i32::MAX.

Final evidence:

- audit/positive-u64-final.raw.txt: 0 of 100 failed, VERIFICATION SUCCESSFUL.
- audit/enum-inverse-final.raw.txt: 0 of 33 failed (3 unreachable),
  VERIFICATION SUCCESSFUL.
- audit/positive-u64-negative.raw.txt: 1 of 13 failed, expected failure.
- audit/enum-negative.raw.txt: 1 of 11 failed, expected failure.
- audit/enum-inverse.raw.txt records the earlier infrastructure-only failure:
  sccache could not set permissions for missing /mnt/c output paths, os error 2.
- audit/enum-inverse-rerun.raw.txt records the wrapper-disabled successful
  enum run before the final negative-control declaration was added.

Checks:

- cargo fmt --check --manifest-path rust/crates/actors/Cargo.toml passed.
- A separate stable cargo check was started offline with an isolated target
  directory but remained blocked behind package-cache/build locks held by
  concurrent unrelated cargo processes; it was stopped after recording the
  lock state. The Kani compilation itself succeeded for every final harness.

Scope limits:

These results prove only the named PositiveU64 and enum mapping properties in
the d690 source closure. They do not prove complete Actors admission,
transport behavior, generated wire roundtrips, or arbitrary SDK behavior.
