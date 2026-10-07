# Kani module path fix and selected PASS receipt

The current Q source previously failed with E0583 because domain.rs is included
inside contract::generated::domain and mod kani_proofs resolved to the actors
source root. The source now contains exactly one Kani-only path attribute:

    #[cfg(kani)]
    #[path = "domain/kani_proofs.rs"]
    mod kani_proofs;

The Q diff for rust/crates/actors/src/domain.rs is one insertion and no other
selected-source change. The repaired current production crate compiled and the
following fully qualified production harnesses passed sequentially:

- contract::generated::domain::kani_proofs::positive_u64_constructor_accepts_exactly_nonzero_values
  (0 of 100 failed; terminal exit 0)
- contract::generated::domain::kani_proofs::subscription_start_valid_wire_round_trip_preserves_oneof_identity
  (0 of 141 failed; terminal exit 0)

The source-bound receipt is receipt.current-fixed.json. The earlier
compile-blocked attempt remains preserved as receipt.current-blocked.json.
The long expected-revision solver PTY 17660 / CBMC PID 1031718 was not
restarted or modified.
