# Symbolic observation/revision Kani prototype

This is an isolated source-only prototype for the selected full-width wire-domain cohort. It is not a production proof receipt and does not alter the frozen Actors proof harness.

The three harnesses quantify only these domains:

- `SubscriptionObservation.delivered_cursor`, `completed_cursor`, and `recoverable_cursor`: all `u64` values.
- `SubscriptionObservation.failed_cursor`: `Option<u64>`, with symbolic presence.
- `ActorObservation.checkpoint_unix_millis`: `Option<u64>`, with symbolic presence.
- `ActorObservation.checkpoint_epoch` and `configuration_revision`: all `u64` values.
- `UpdateActorRequest.expected_configuration_revision`: all `u64` values.

Each harness fixes all unrelated IDs, strings, digest, enum, limits, bindings, and collections to known-valid values. The intended property is exact semantic accessor equality plus generated-wire round-trip equality, including `Option` presence. It does not claim universal validity of IDs, strings, aggregates, transport, FFI, generated SDKs, or packaging.

## Reproduction

Use Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, one job, and unwind 1. Run each harness in a separate invocation:

```text
cargo-kani --manifest-path source/rust/crates/actors/Cargo.toml --package acyclic-actors --harness domain::kani_proofs::subscription_observation_preserves_full_width_cursors_and_presence --exact --default-unwind 1 --harness-timeout 300 -Z unstable-options -j1
cargo-kani --manifest-path source/rust/crates/actors/Cargo.toml --package acyclic-actors --harness domain::kani_proofs::actor_observation_and_optional_response_preserve_u64_fields --exact --default-unwind 1 --harness-timeout 300 -Z unstable-options -j1
cargo-kani --manifest-path source/rust/crates/actors/Cargo.toml --package acyclic-actors --harness domain::kani_proofs::update_request_preserves_full_width_configuration_revision --exact --default-unwind 1 --harness-timeout 300 -Z unstable-options -j1
```

The receipt records the observed runs. The explicit invocations, including a reduced clone-free subscription projection harness, remained in Cargo metadata access on the shared registry mount before harness checking. The earlier combined attempt reached CBMC only for the final update harness and was stopped during propositional reduction. No verification marker was produced. Therefore this prototype is an execution diagnostic and mathematical property design, not a successful qualification.

No `target/`, Cargo cache, or generated binary is part of this research copy.

