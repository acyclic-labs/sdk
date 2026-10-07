# Theorem map for the current derived producer

All rows below are preparation entries. A PASS requires a fresh terminal Kani
receipt bound to the frozen current source inventory.

| Harness or prepared harness | Direct production function/conversion | Exact claim |
|---|---|---|
| actor_id_nominal_fixed_witnesses_preserve_identity | ActorId::try_from(String), ActorId::as_str, From<ActorId> for String | For the listed non-empty witnesses, construction succeeds and the exact string is returned; this is a finite witness property, not an arbitrary-string theorem. |
| actor_id_nominal_rejects_empty | ActorId::try_from(String) | The empty string returns DomainError::EmptyActorId. |
| positive_u64_constructor_accepts_exactly_nonzero_values | PositiveU64::new, From<PositiveU64> for u64 | For every symbolic u64, zero is rejected, every nonzero value is accepted, and the value round-trips exactly. |
| subscription_start_preserves_cursor_and_current_head_presence | SubscriptionStart::try_from(wire::SubscriptionStart) | Every symbolic cursor u64 is preserved; CurrentHead(true) is distinct and accepted; missing and CurrentHead(false) are rejected. |
| subscription_start_valid_wire_round_trip_preserves_oneof_identity | Production TryFrom followed by generated From | Valid cursor and current-head oneof cases round-trip without changing variant or cursor payload. |
| subscription_observation_try_from_preserves_u64_and_failed_cursor_presence | SubscriptionObservation::try_from and generated From | All four cursor fields preserve full u64; failed_cursor preserves None versus Some(value). Fixed non-empty strings are only preconditions. |
| actor_observation_try_from_preserves_u64_presence_and_fixed_bytes | ActorObservation::try_from and generated From | Optional checkpoint time, checkpoint epoch, and configuration revision preserve full u64; valid fixed 32-byte digest and known enum are preconditions. |
| actor_response_try_from_preserves_optional_actor_presence | Generated CreateActorResponse::try_from and UpdateActorResponse::try_from | None remains absent and Some(valid ActorObservation) remains present through conversion and wire round-trip. |

The existing production harness names are recorded in
harnesses/current-existing-production-harnesses.txt. The only new source
snippet is the nominal-ID finite-witness preparation. No row is a proof of
arbitrary strings, transport behavior, or the entire Actors API.
