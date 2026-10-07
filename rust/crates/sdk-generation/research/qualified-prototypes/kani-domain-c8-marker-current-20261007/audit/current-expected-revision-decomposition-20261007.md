# Current c8 expected-revision decomposition (2026-10-07)

This evidence binds to the actual Actors production source in `Q:\sdk\work\sdkgen-actors-c8-minimal`. The direct production conversion harness calls the generated `UpdateActorRequest::try_from` and then the production `expected_configuration_revision()` accessor. It does not duplicate either implementation.

## Source binding

The proof target was built with Kani 0.68, CBMC 6.11.0, and `nightly-2026-08-21`, offline, one build job, `--default-unwind 2`, `--slice-formula`, and CaDiCaL. Generated-facade commits advanced while the run was active, so the source identity is the SHA-256 inventory in `receipt.current-decomposition.json`, rather than HEAD alone. Latest observed HEAD is `1ac5b398644d9d7056a95cd579c6a5315dd98db8`; theorem binding remains the exact file hash inventory.

Relevant production files:

- `domain.rs`: `361C8584C3C9C5A4E1974B97B9341226D4A2C816A1E1684D18A3BE5D8BFDB257`
- `domain/kani_proofs.rs`: `FC6C30F64E9056E8D1C80A5CB15AF90824B00A301F68D99DACF1ACE388F4CD57`
- `contract.rs`: `735E6CE63A46319700A92C08C32E0AB1FFD35CB99630369BBB05C5C6F5B79275`
- `wire.rs`: `5773FBB750C9FAB97F177C86FD816D3EAEC175C806E30AC4C8ADD07DF363BD77`
- `lib.rs`: `12953671CAD77738DB59068976FFAFB47121BEEDD27B215D6B57F99CE9A5F68A`

## PASS: conversion preservation

`update_request_expected_revision_only_through_production_conversion` quantifies an arbitrary `u64` expected revision, constructs one fixed valid wire fixture with that value, calls the generated production `UpdateActorRequest::try_from`, and asserts the production accessor equals the original value. Kani reports `0 of 1623 failed (52 unreachable)`, `VERIFICATION:- SUCCESSFUL`, and `Complete - 1 successfully verified harnesses, 0 failures, 1 total` in 4.8720174 seconds. Raw evidence is `audit/update-request-conversion-only-kani068.raw.txt` (SHA-256 `547B73A1BE907732679F49BDA922498573D7768831403FA8B41E991A22777814`).

The mathematical claim is limited to this fixed-fixture ingress/conversion path and all `u64` revision values. It says nothing about arbitrary strings, arbitrary binding lists, admission, transport, or whole-wire behavior.

## Admission probes: no PASS claimed

The one-binding production `validate_update` probe, the zero-binding production `validate_update` probe, and an earlier combined decomposition probe each reached CBMC but were interrupted after bounded observation with no terminal verification summary. Their raw logs are retained and the receipt marks each `inconclusive_interrupted`; they are not proof results. The one-binding path reached approximately 531k variables / 1.335m clauses before plateau. The zero-binding fixture still reached approximately 1.175m variables / 4.261m clauses because the fixture construction and clear remain in symbolic execution.

The duplicate-binding negative harness is source-present for future bounded work but was not launched. No admission, uniqueness, arbitrary-string, or broad transport claim is made.

## Process preservation

The original Actors solver was never signalled or restarted: PTY 17660, CBMC PID 1031718, historical source checkout `Q:\sdk\work\sdkgen-main-actual03bb`. It remained live at receipt capture. These probes used a separate task-local target and were stopped only through their own PTY.
## Targeted admission rule proofs

The following harnesses call the maintained production `validate_update` directly. Their assertions isolate one admission rule; every other request field is a fixed valid witness, except where stated. `expected_configuration_revision` remains arbitrary `u64` in every harness, but it is not itself an admission condition.

- `update_request_production_admission_rejects_missing_limits`: arbitrary revision, valid scalar fields, empty bindings, and `limits = None`; production result is `InvalidArgument`. Kani: `0 of 7954 failed (553 unreachable)`, 2.736311s.
- `update_request_production_admission_rejects_empty_actor_id`: arbitrary revision, all other fields valid, empty actor id; production result is `InvalidArgument`. Kani: `0 of 7968 failed (553 unreachable)`, 2.7375867s.
- `update_request_production_admission_rejects_zero_digest`: arbitrary revision, all other fields valid, exactly 32 zero digest bytes; production result is `InvalidArgument`. Kani: `0 of 7954 failed (553 unreachable)`, 19.094551s, unwind 65 for the production 32-byte iterator. The earlier unwind-2 run failed only its unwinding assertion and is retained separately; it was not counted as a theorem.
- `update_request_production_admission_rejects_empty_idempotency_key`: arbitrary revision, all other fields valid, empty idempotency key; production result is `InvalidArgument`. Kani: `0 of 7968 failed (553 unreachable)`, 4.2163796s.
- `update_request_production_admission_rejects_zero_handler_timeout_for_any_other_limits`: arbitrary revision, arbitrary `memory_bytes` and `checkpoint_bytes`, zero handler timeout, all other fields valid; production result is `InvalidArgument`. Kani: `0 of 7954 failed (553 unreachable)`, 3.0748546s.

These are rule-local theorems. They do not prove acceptance for arbitrary request shapes, arbitrary UTF-8/string contents, binding limits or uniqueness, or the complete `validate_update` predicate. The empty-bindings acceptance probe still reached a CBMC plateau and remains inconclusive; its raw log is preserved.

The one-binding and zero-binding admission interruptions used separate CBMC processes (historically observed PIDs 2122406 and 2125802) and were stopped through their own PTYs. The original expected-revision solver PTY 17660 / CBMC PID 1031718 remains live and untouched.

## Current nominal constructor checks

The same current c8 source-bound Kani module also passed the two smallest nominal domain obligations:

- `positive_u64_constructor_accepts_exactly_nonzero_values`: arbitrary `u64`; actual production `PositiveU64::new` accepts exactly nonzero values and `get()` preserves the input (`0 of 100 failed`).
- `actor_limits_constructor_accepts_exactly_positive_values`: arbitrary handler, memory, and checkpoint `u64` values; actual production `ActorLimits::new` accepts exactly the all-positive tuple and accessors preserve all three values (`0 of 127 failed`).

Existing source-bound harnesses cover the generated oneof/presence and roundtrip obligations for subscription starts, failed cursors, Actor checkpoint/configuration fields, optional Create/Update response actors, and enum unknown-state rejection. No additional smallest direct-production nominal gap was identified in this review.
