# Signed Actors bounded direct-production presence proofs

This research artifact is bound to the exact signed Actors source checkout:

- checkout: `C:/Users/varun/.codex/tmp/sdkgen-actors-c8-minimal-1269511`
- revision: `1269511d636b8ceeea1604a23919a9c1448241fa`
- production `rust/crates/actors/src/domain.rs` SHA-256: `361C8584C3C9C5A4E1974B97B9341226D4A2C816A1E1684D18A3BE5D8BFDB257`

The proof-only additions are in the task-local `domain/kani_proofs.rs` checkout. They call production `TryFrom` implementations and production accessors directly. The additions construct concrete bounded strings/vectors and leave only selected `u64` values and `Option` presence tags symbolic. They do not copy a conversion or validation algorithm.

Pinned Kani 0.68.0 / CBMC 6.11.0 / nightly-2026-08-21 / CaDiCaL 3.0.0 run with one build job and target `/tmp/kani-actors-signed-bounded-presence-20261007` completed:

- `subscription_observation_bounded_direct_u64_and_failed_cursor_presence`: `0 of 241 failed (4 unreachable)`
- `actor_observation_bounded_direct_u64_presence_and_fixed_digest`: `0 of 1684 failed (56 unreachable)`
- `actor_response_bounded_direct_optional_actor_presence`: `0 of 1648 failed (56 unreachable)`

The terminal summary is `Complete - 3 successfully verified harnesses, 0 failures, 3 total.` Raw output is `audit-signed-bounded-final.raw.txt`; receipt is `receipt.json`.

The theorems are narrow. They cover exact preservation of arbitrary selected `u64` values and symbolic optional presence over fixed valid production fixtures. They do not quantify arbitrary strings, arbitrary repeated messages, arbitrary digests, full wire roundtrips, transport behavior, or UpdateActorResponse.

The original broad fixture harnesses remain retained in the signed module and remain inconclusive under CBMC's `alloc::handle_alloc_error` unsupported construct. The bounded proofs avoid symbolic allocation sizes; this is an explicit harness assumption, not an unsound allocator ignore.
