//! Small, pure Kani harnesses for Rust-owned Actors invariants.
//!
//! This module is compiled only by `cargo kani`. It is a child of `domain`,
//! so it can call the existing crate-private predicate without changing its
//! visibility or creating a second validation rule.

use super::{
    ActorLimits, ActorObservation, ActorState, CodeSha256, CreateActorResponse, DomainError,
    ErrorCode, PositiveU64, SubscriptionObservation, SubscriptionStart, SubscriptionState,
    UpdateActorRequest, UpdateActorResponse, valid_code_sha256, wire,
};

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_fixed_length_matches_contract() {
    let bytes: [u8; 32] = kani::any();
    // This is the admission invariant consumed by `CodeSha256::new`; keep the
    // Vec conversion and newtype construction in runtime property tests.
    assert_eq!(valid_code_sha256(&bytes), bytes != [0; 32]);
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_constructor_preserves_valid_bytes() {
    // Kani's maintained exact_vec model gives a symbolic, fixed-length Vec;
    // this avoids an unbounded allocation while exercising the real
    // CodeSha256::new conversion and accessor.
    let input = kani::vec::exact_vec::<u8, 32>();
    kani::assume(input.iter().any(|byte| *byte != 0));

    let digest = CodeSha256::new(input.clone()).expect("non-zero 32-byte input is valid");
    assert_eq!(digest.as_bytes(), input.as_slice());
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_rejects_empty() {
    let bytes: [u8; 0] = kani::any();
    assert!(!valid_code_sha256(&bytes));
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_rejects_31_bytes() {
    let bytes: [u8; 31] = kani::any();
    assert!(!valid_code_sha256(&bytes));
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_rejects_33_bytes() {
    let bytes: [u8; 33] = kani::any();
    assert!(!valid_code_sha256(&bytes));
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_rejects_64_bytes() {
    let bytes: [u8; 64] = kani::any();
    assert!(!valid_code_sha256(&bytes));
}

#[kani::proof]
#[kani::unwind(65)]
fn code_sha256_predicate_accepts_only_nonzero_32_byte_prefixes() {
    let bytes: [u8; 64] = kani::any();
    let length: usize = kani::any();
    kani::assume(length <= bytes.len());
    let candidate = &bytes[..length];
    let expected = length == 32 && candidate.iter().any(|byte| *byte != 0);
    assert_eq!(valid_code_sha256(candidate), expected);
}

fn assert_subscription_state_mapping(raw: i32) {
    match SubscriptionState::try_from(raw) {
        Ok(value) => assert_eq!(i32::from(value), raw),
        Err(DomainError::UnknownSubscriptionState(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
    }
}

fn assert_actor_state_mapping(raw: i32) {
    match ActorState::try_from(raw) {
        Ok(value) => assert_eq!(i32::from(value), raw),
        Err(DomainError::UnknownActorState(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
    }
}

fn assert_error_code_mapping(raw: i32) {
    match ErrorCode::try_from(raw) {
        Ok(value) => assert_eq!(i32::from(value), raw),
        Err(DomainError::UnknownErrorCode(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn subscription_start_preserves_cursor_and_current_head_presence() {
    let cursor: u64 = kani::any();
    let cursor_wire = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::Cursor(cursor)),
    };
    let parsed_cursor = SubscriptionStart::try_from(cursor_wire).expect("cursor is valid");
    assert_eq!(parsed_cursor.cursor_value(), Some(cursor));
    assert_eq!(parsed_cursor.current_head_value(), None);

    let current_head_wire = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::CurrentHead(true)),
    };
    let parsed_current_head =
        SubscriptionStart::try_from(current_head_wire).expect("true current head is valid");
    assert_eq!(parsed_current_head.cursor_value(), None);
    assert_eq!(parsed_current_head.current_head_value(), Some(true));

    let false_current_head = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::CurrentHead(false)),
    };
    assert_eq!(
        SubscriptionStart::try_from(false_current_head),
        Err(DomainError::InvalidSubscription)
    );

    let missing_start = wire::SubscriptionStart { start: None };
    assert_eq!(
        SubscriptionStart::try_from(missing_start),
        Err(DomainError::InvalidSubscription)
    );
}

#[kani::proof]
#[kani::unwind(1)]
fn subscription_start_valid_wire_round_trip_preserves_oneof_identity() {
    let cursor: u64 = kani::any();
    let cursor_wire = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::Cursor(cursor)),
    };
    let parsed_cursor = SubscriptionStart::try_from(cursor_wire.clone()).expect("cursor is valid");
    let cursor_round_trip: wire::SubscriptionStart = parsed_cursor.into();
    assert_eq!(cursor_round_trip, cursor_wire);

    let current_head_wire = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::CurrentHead(true)),
    };
    let parsed_current_head =
        SubscriptionStart::try_from(current_head_wire.clone()).expect("true current head is valid");
    let current_head_round_trip: wire::SubscriptionStart = parsed_current_head.into();
    assert_eq!(current_head_round_trip, current_head_wire);
}

#[kani::proof]
#[kani::unwind(2)]
fn subscription_observation_try_from_preserves_u64_and_failed_cursor_presence() {
    let delivered_cursor: u64 = kani::any();
    let completed_cursor: u64 = kani::any();
    let recoverable_cursor: u64 = kani::any();
    let failed_cursor_value: u64 = kani::any();
    let failed_cursor_present: bool = kani::any();
    let failed_cursor = if failed_cursor_present {
        Some(failed_cursor_value)
    } else {
        None
    };
    let wire_value = wire::SubscriptionObservation {
        subscription_id: "subscription".to_owned(),
        stream_path: "stream".to_owned(),
        state: 1,
        delivered_cursor,
        completed_cursor,
        recoverable_cursor,
        placement_anchor: true,
        retry_count: 2,
        failure_code: "none".to_owned(),
        failed_cursor,
    };

    let parsed = SubscriptionObservation::try_from(wire_value.clone())
        .expect("known subscription state is accepted");
    assert_eq!(parsed.delivered_cursor(), delivered_cursor);
    assert_eq!(parsed.completed_cursor(), completed_cursor);
    assert_eq!(parsed.recoverable_cursor(), recoverable_cursor);
    assert_eq!(parsed.failed_cursor(), failed_cursor);
    let round_trip: wire::SubscriptionObservation = parsed.into();
    assert_eq!(round_trip, wire_value);
}

#[kani::proof]
#[kani::unwind(2)]
fn actor_observation_try_from_preserves_u64_presence_and_fixed_bytes() {
    let digest: [u8; 32] = kani::any();
    let checkpoint_unix_millis_value: u64 = kani::any();
    let checkpoint_present: bool = kani::any();
    let checkpoint_unix_millis = if checkpoint_present {
        Some(checkpoint_unix_millis_value)
    } else {
        None
    };
    let checkpoint_epoch: u64 = kani::any();
    let configuration_revision: u64 = kani::any();
    let wire_value = wire::ActorObservation {
        actor_id: "actor".to_owned(),
        code_sha256: digest.to_vec().into(),
        home_region: "region".to_owned(),
        state: 1,
        subscriptions: vec![],
        checkpoint_unix_millis,
        checkpoint_epoch,
        configuration_revision,
    };

    match ActorObservation::try_from(wire_value.clone()) {
        Ok(parsed) => {
            assert_ne!(digest, [0; 32]);
            assert_eq!(parsed.code_sha256().as_bytes(), &digest);
            assert_eq!(parsed.checkpoint_unix_millis(), checkpoint_unix_millis);
            assert_eq!(parsed.checkpoint_epoch(), checkpoint_epoch);
            assert_eq!(parsed.configuration_revision(), configuration_revision);
            let round_trip: wire::ActorObservation = parsed.into();
            assert_eq!(round_trip, wire_value);
        }
        Err(DomainError::InvalidCodeSha256) => assert_eq!(digest, [0; 32]),
        Err(_) => assert!(false),
    }
}

#[kani::proof]
#[kani::unwind(2)]
fn actor_response_try_from_preserves_optional_actor_presence() {
    let empty_create = wire::CreateActorResponse { actor: None };
    let parsed_empty_create = CreateActorResponse::try_from(empty_create.clone())
        .expect("an absent create response actor is valid");
    assert!(parsed_empty_create.actor().is_none());
    let empty_create_round_trip: wire::CreateActorResponse = parsed_empty_create.into();
    assert_eq!(empty_create_round_trip, empty_create);

    let actor = wire::ActorObservation {
        actor_id: "actor".to_owned(),
        code_sha256: vec![1; 32].into(),
        home_region: "region".to_owned(),
        state: 1,
        subscriptions: vec![],
        checkpoint_unix_millis: None,
        checkpoint_epoch: u64::MAX,
        configuration_revision: u64::MAX,
    };
    let create_with_actor = wire::CreateActorResponse {
        actor: Some(actor.clone()),
    };
    let parsed_create = CreateActorResponse::try_from(create_with_actor.clone())
        .expect("a valid create response actor is accepted");
    assert!(parsed_create.actor().is_some());
    let create_round_trip: wire::CreateActorResponse = parsed_create.into();
    assert_eq!(create_round_trip, create_with_actor);

    let update_with_actor = wire::UpdateActorResponse { actor: Some(actor) };
    let parsed_update = UpdateActorResponse::try_from(update_with_actor.clone())
        .expect("a valid update response actor is accepted");
    assert!(parsed_update.actor().is_some());
    let update_round_trip: wire::UpdateActorResponse = parsed_update.into();
    assert_eq!(update_round_trip, update_with_actor);
}

#[kani::proof]
#[kani::unwind(2)]
fn update_request_try_from_preserves_expected_revision() {
    let expected_configuration_revision: u64 = kani::any();
    let wire_value = wire::UpdateActorRequest {
        actor_id: "actor".to_owned(),
        code_sha256: vec![1; 32].into(),
        bindings: vec![wire::Binding {
            name: "binding".to_owned(),
            capability: "capability".to_owned(),
            resource: "resource".to_owned(),
        }],
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 2,
            checkpoint_bytes: 3,
        }),
        expected_configuration_revision,
        idempotency_key: "idempotency".to_owned(),
    };

    let parsed = UpdateActorRequest::try_from(wire_value.clone())
        .expect("the fixed valid update request passes canonical validation");
    assert_eq!(
        parsed.expected_configuration_revision(),
        expected_configuration_revision
    );
    let round_trip: wire::UpdateActorRequest = parsed.into();
    assert_eq!(round_trip, wire_value);
}

#[kani::proof]
#[kani::unwind(2)]
fn subscription_observation_try_from_rejects_unknown_state() {
    let raw_state: i32 = kani::any();
    let wire_value = wire::SubscriptionObservation {
        subscription_id: "subscription".to_owned(),
        stream_path: "stream".to_owned(),
        state: raw_state,
        delivered_cursor: 0,
        completed_cursor: 0,
        recoverable_cursor: 0,
        placement_anchor: false,
        retry_count: 0,
        failure_code: "none".to_owned(),
        failed_cursor: None,
    };

    match SubscriptionObservation::try_from(wire_value) {
        Ok(_) => assert!(raw_state == 0 || raw_state == 1 || raw_state == 2),
        Err(DomainError::UnknownSubscriptionState(found)) => assert_eq!(found, raw_state),
        Err(_) => assert!(false),
    }
}

#[kani::proof]
#[kani::unwind(2)]
fn actor_observation_try_from_rejects_unknown_state() {
    let raw_state: i32 = kani::any();
    let wire_value = wire::ActorObservation {
        actor_id: "actor".to_owned(),
        code_sha256: vec![1; 32].into(),
        home_region: "region".to_owned(),
        state: raw_state,
        subscriptions: vec![],
        checkpoint_unix_millis: None,
        checkpoint_epoch: 0,
        configuration_revision: 0,
    };

    match ActorObservation::try_from(wire_value) {
        Ok(_) => assert!(raw_state == 0 || raw_state == 1 || raw_state == 2 || raw_state == 3),
        Err(DomainError::UnknownActorState(found)) => assert_eq!(found, raw_state),
        Err(_) => assert!(false),
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn positive_u64_constructor_accepts_exactly_nonzero_values() {
    let value: u64 = kani::any();
    match PositiveU64::new(value) {
        Ok(positive) => {
            assert_ne!(value, 0);
            assert_eq!(positive.get(), value);
        }
        Err(error) => {
            assert_eq!(value, 0);
            assert_eq!(
                error,
                DomainError::Contract(crate::ContractError::InvalidArgument)
            );
        }
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn actor_limits_constructor_accepts_exactly_positive_values() {
    let handler_timeout_millis: u64 = kani::any();
    let memory_bytes: u64 = kani::any();
    let checkpoint_bytes: u64 = kani::any();

    match ActorLimits::new(handler_timeout_millis, memory_bytes, checkpoint_bytes) {
        Ok(limits) => {
            assert_ne!(handler_timeout_millis, 0);
            assert_ne!(memory_bytes, 0);
            assert_ne!(checkpoint_bytes, 0);
            assert_eq!(limits.handler_timeout_millis(), handler_timeout_millis);
            assert_eq!(limits.memory_bytes(), memory_bytes);
            assert_eq!(limits.checkpoint_bytes(), checkpoint_bytes);
        }
        Err(error) => {
            assert_eq!(
                error,
                DomainError::Contract(crate::ContractError::InvalidArgument)
            );
            assert!(handler_timeout_millis == 0 || memory_bytes == 0 || checkpoint_bytes == 0);
        }
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn enum_numeric_mappings_are_inverse_and_lossless() {
    let subscription_raw: i32 = kani::any();
    assert_subscription_state_mapping(subscription_raw);

    let actor_raw: i32 = kani::any();
    assert_actor_state_mapping(actor_raw);

    let error_raw: i32 = kani::any();
    assert_error_code_mapping(error_raw);
}
