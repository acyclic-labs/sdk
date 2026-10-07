# Signed Actors 1269511 direct-production qualification

Source checkout: `Q:/sdk/work/sdkgen-actors-c8-minimal-signed`.

The signed review commit is `1269511d636b8ceeea1604a23919a9c1448241fa`. The checkout advanced to `919a04d83a265d90f1bce7ede2dfdbfa301829a9` with a pinned-formatting commit while the run was prepared; the production `domain.rs` and `domain/kani_proofs.rs` blobs used by the rule-local run remain byte-identical to the signed commit:

- `domain.rs`: `361C8584C3C9C5A4E1974B97B9341226D4A2C816A1E1684D18A3BE5D8BFDB257`
- `domain/kani_proofs.rs`: `0D4C0F6F7D95111E6727D6E7AE885EF755B42887C3439B9AF57EE9211154F576`

Toolchain: Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, CaDiCaL 3.0.0, default unwind 2. Raw output is `signed-1269511-rulelocal-kani068.raw.txt`; receipt is `../receipt.signed-1269511-rulelocal.json`.

The rule-local command launched nine signed harnesses. Six completed successfully: `PositiveU64::new` exact nonzero acceptance/preservation; `ActorLimits::new` exact all-positive acceptance/preservation; valid `SubscriptionStart` cursor/`CurrentHead(true)` oneof roundtrip; unknown subscription-state rejection; unknown actor-state rejection; and inverse/lossless enum numeric mappings. CBMC reports zero failed checks for each successful harness.

Three direct production conversion/presence harnesses did not produce a theorem: `subscription_observation_try_from_preserves_u64_and_failed_cursor_presence`, `actor_observation_try_from_preserves_u64_presence_and_fixed_bytes`, and `actor_response_try_from_preserves_optional_actor_presence`. Each reached CBMC's `alloc::handle_alloc_error` `unsupported_construct`; the final command summary reports 3 failures. These are inconclusive tool/model limitations, not mathematical counterexamples. They are not counted as PASS.

The signed combined projection harness `update_request_expected_revision_only_after_production_admission` is launched separately in `signed-1269511-expected-revision-kani068.raw.txt` under PTY 44535, target `/tmp/kani-signed-1269511`, and remains solver-active at the time this record was written. It calls the actual production `validate_update`, `UpdateActorRequest::try_from`, and `expected_configuration_revision()` accessor; its fixture fixes all fields except an arbitrary `u64` expected revision. No completion claim is made until a terminal Kani summary is present.

The signed canonical acceptance harness `update_request_production_admission_accepts_canonical_fixture` has no terminal proof in this record and remains inconclusive/unqualified. The prior current-c8 acceptance interruptions are not transferred to the signed source. No arbitrary-string, arbitrary-binding-list, transport, or whole-request theorem is claimed.

## Bounded direct-production closure

A proof-only bounded fixture snapshot was run from exact signed revision `1269511d636b8ceeea1604a23919a9c1448241fa` at `C:/Users/varun/.codex/tmp/sdkgen-actors-c8-minimal-1269511`. Production `domain.rs` remained hash `361C8584C3C9C5A4E1974B97B9341226D4A2C816A1E1684D18A3BE5D8BFDB257`; only the task-local Kani proof module gained three harnesses. With concrete bounded strings/vectors and arbitrary selected `u64` values/presence tags, the actual production `TryFrom` and accessors verified:

- `subscription_observation_bounded_direct_u64_and_failed_cursor_presence`: `0 of 241 failed (4 unreachable)`.
- `actor_observation_bounded_direct_u64_presence_and_fixed_digest`: `0 of 1684 failed (56 unreachable)`.
- `actor_response_bounded_direct_optional_actor_presence`: `0 of 1648 failed (56 unreachable)`.

Terminal summary: `Complete - 3 successfully verified harnesses, 0 failures, 3 total.` Raw SHA-256: `DE98AD5E324038176B7BFE114D644BC8FD2FB54693CB815675C0C9E6BCC673EC`. Receipt and the minimal harness snapshot are in `kani-actors-signed-bounded-presence-20261007`.

These PASS results quantify only the stated fixed-fixture/bounded-allocation ingress properties. They do not upgrade arbitrary-string, arbitrary-repeated-field, full roundtrip, transport, or UpdateActorResponse claims. The broad original fixtures remain separately inconclusive because of Kani/CBMC allocator unsupported constructs.
