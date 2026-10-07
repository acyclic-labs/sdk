# Maintained cutover ingress obligation map

This artifact targets the current Q source tree at commit `03bbf867c32ab61dfb262ab20fdf9d31a4e6dae1`. The tree is dirty; source file hashes, rather than HEAD alone, define the compiled source. No transport behavior, arbitrary-string theorem, generated SDK theorem, or whole-message clone claim is made.

## Production implementation to theorem mapping

| Harness | Production implementation invoked | Exact theorem |
|---|---|---|
| `subscription_start_preserves_cursor_and_current_head_presence` | `SubscriptionStart::try_from(wire::SubscriptionStart)` | For every `u64` cursor, `Cursor(cursor)` preserves the exact value and remains distinct from `CurrentHead(true)`; absent oneof and `CurrentHead(false)` are rejected. |
| `subscription_start_valid_wire_round_trip_preserves_oneof_identity` | `SubscriptionStart::try_from` then `From<SubscriptionStart> for wire::SubscriptionStart` | Valid cursor and current-head oneof cases round-trip without changing the selected variant or cursor value. |
| `subscription_observation_try_from_preserves_u64_and_failed_cursor_presence` | `SubscriptionObservation::try_from(wire::SubscriptionObservation)` then its `From` implementation | Every symbolic `delivered_cursor`, `completed_cursor`, `recoverable_cursor`, and optional `failed_cursor: Option<u64>` is copied exactly, including `None` versus `Some(value)`. State and strings are fixed valid inputs; no arbitrary-string claim. |
| `actor_observation_try_from_preserves_u64_presence_and_fixed_bytes` | `ActorObservation::try_from(wire::ActorObservation)` | The production ingress preserves symbolic `checkpoint_unix_millis: Option<u64>`, `checkpoint_epoch`, `configuration_revision`, and every valid symbolic 32-byte digest; the all-zero digest is rejected by the production `CodeSha256` path. |
| `actor_response_try_from_preserves_optional_actor_presence` | Macro-generated `CreateActorResponse::try_from`, `UpdateActorResponse::try_from`, and `actor_response` helper | `None` remains absent and `Some(valid ActorObservation)` remains present for both response types; both wire round-trips preserve the optional message identity. |
| `update_request_try_from_preserves_expected_revision` | `UpdateActorRequest::try_from`, which calls production `crate::validate_update` | For every symbolic `expected_configuration_revision: u64` under fixed valid request fields, canonical validation succeeds and the revision is copied exactly by the semantic ingress and wire round-trip. This harness includes the maintained validator and may be solver-heavy. |
| `subscription_observation_try_from_rejects_unknown_state` | `SubscriptionObservation::try_from` -> `SubscriptionState::try_from` | Unknown raw subscription enum integers produce `DomainError::UnknownSubscriptionState(raw)`; known 0, 1, and 2 are not rejected by this mapping. |
| `actor_observation_try_from_rejects_unknown_state` | `ActorObservation::try_from` -> `ActorState::try_from` | Unknown raw actor enum integers produce `DomainError::UnknownActorState(raw)`; known 0 through 3 are not rejected by this mapping. |
| `enum_numeric_mappings_are_inverse_and_lossless` | Direct production `TryFrom<i32>` and `From<enum> for i32` implementations | All published `SubscriptionState`, `ActorState`, and `ErrorCode` values round-trip exactly; unknown integers remain explicit typed errors. |

Existing digest, PositiveU64, and ActorLimits harnesses remain in the same production proof module. Their scope is constructor/predicate behavior; they do not substitute for the ingress harnesses above.

## Negative controls

The source contains explicit negative branches for missing/false subscription oneof values, zero positive values, invalid digest lengths, all-zero digest ingress, unknown subscription state ingress, and unknown actor state ingress. The current run is one live Kani invocation; no production function was replaced with a proof-only algorithm.

## Current run state

The raw run is `audit/domain-ingress-current-kani068.raw.txt`. At the time of this audit, enum mapping, ActorLimits, PositiveU64, actor unknown-state ingress, and subscription unknown-state ingress had completed successfully. `update_request_try_from_preserves_expected_revision` was still being solved by the same CBMC process; no PASS is claimed for the overall augmented set until the terminal summary and exit marker are present.
