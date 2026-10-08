//! Direct proofs of nominal admission, wire enum identities, and numeric projections.
//!
//! Observation harnesses quantify full-width numeric values and optional presence
//! through production conversions and getters, with fixed valid nonnumeric context.
//! Their fresh solver runs are pending; historical four-harness receipts do not
//! prove these additions, whole DTOs, serialization, transport, or service behavior.

use super::{ActorLimits, ActorState, DomainError, ErrorCode, PositiveU64, SubscriptionStart, SubscriptionState};

fn assert_subscription_state_mapping(raw: i32) {
    let expected = raw == SubscriptionState::Unspecified as i32
        || raw == SubscriptionState::Active as i32
        || raw == SubscriptionState::Paused as i32;
    let result = super::parse_subscription_state(raw);
    assert_eq!(result.is_ok(), expected);
    match result {
        Ok(value) => assert_eq!(super::encode_subscription_state(value), raw),
        Err(DomainError::UnknownSubscriptionState(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
    }
}

fn assert_actor_state_mapping(raw: i32) {
    let expected = raw == ActorState::Unspecified as i32
        || raw == ActorState::Active as i32
        || raw == ActorState::Hibernated as i32
        || raw == ActorState::Paused as i32;
    let result = super::parse_actor_state(raw);
    assert_eq!(result.is_ok(), expected);
    match result {
        Ok(value) => assert_eq!(super::encode_actor_state(value), raw),
        Err(DomainError::UnknownActorState(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
    }
}

fn assert_error_code_mapping(raw: i32) {
    let expected = raw == ErrorCode::Unspecified as i32
        || raw == ErrorCode::InvalidArgument as i32
        || raw == ErrorCode::CapabilityDenied as i32
        || raw == ErrorCode::CapabilityExpired as i32
        || raw == ErrorCode::ActorNotFound as i32
        || raw == ErrorCode::SubscriptionNotFound as i32
        || raw == ErrorCode::IdempotencyMismatch as i32
        || raw == ErrorCode::Conflict as i32
        || raw == ErrorCode::AdmissionDenied as i32
        || raw == ErrorCode::CheckpointFailed as i32
        || raw == ErrorCode::DependencyUnavailable as i32;
    let result = super::parse_error_code(raw);
    assert_eq!(result.is_ok(), expected);
    match result {
        Ok(value) => assert_eq!(super::encode_error_code(value), raw),
        Err(DomainError::UnknownErrorCode(found)) => assert_eq!(found, raw),
        _ => {
            assert!(false);
        }
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

#[kani::proof]
#[kani::unwind(1)]
fn subscription_start_ingress_and_payload_are_lossless() {
    let selector: u8 = kani::any();
    kani::assume(selector < 4);
    let cursor: u64 = kani::any();
    let start = match selector {
        0 => None,
        1 => Some(crate::wire::subscription_start::Start::Cursor(cursor)),
        2 => Some(crate::wire::subscription_start::Start::CurrentHead(false)),
        _ => Some(crate::wire::subscription_start::Start::CurrentHead(true)),
    };
    let result = SubscriptionStart::try_from(crate::wire::SubscriptionStart { start });
    match result {
        Ok(value) => {
            assert!(selector == 1 || selector == 3);
            if selector == 1 {
                assert_eq!(value.cursor_value(), Some(cursor));
                assert_eq!(value.current_head_value(), None);
            } else {
                assert_eq!(value.cursor_value(), None);
                assert_eq!(value.current_head_value(), Some(true));
            }
        }
        Err(error) => {
            assert!(selector == 0 || selector == 2);
            assert_eq!(error, DomainError::InvalidSubscription);
        }
    }
}

#[kani::proof]
#[kani::unwind(40)]
fn subscription_observation_numeric_projection_is_lossless() {
    let delivered_cursor: u64 = kani::any();
    let completed_cursor: u64 = kani::any();
    let recoverable_cursor: u64 = kani::any();
    let failed_cursor: Option<u64> = kani::any();
    let value = super::SubscriptionObservation::try_from(crate::wire::SubscriptionObservation {
        subscription_id: "s".into(),
        stream_path: "p".into(),
        state: SubscriptionState::Active as i32,
        delivered_cursor,
        completed_cursor,
        recoverable_cursor,
        placement_anchor: false,
        retry_count: 0,
        failure_code: String::new(),
        failed_cursor,
    })
    .unwrap();
    assert_eq!(value.delivered_cursor(), delivered_cursor);
    assert_eq!(value.completed_cursor(), completed_cursor);
    assert_eq!(value.recoverable_cursor(), recoverable_cursor);
    assert_eq!(value.failed_cursor(), failed_cursor);
    let wire: crate::wire::SubscriptionObservation = value.into();
    assert_eq!(wire.delivered_cursor, delivered_cursor);
    assert_eq!(wire.completed_cursor, completed_cursor);
    assert_eq!(wire.recoverable_cursor, recoverable_cursor);
    assert_eq!(wire.failed_cursor, failed_cursor);
}

fn observation(
    checkpoint_unix_millis: Option<u64>,
    checkpoint_epoch: u64,
    configuration_revision: u64,
) -> crate::wire::ActorObservation {
    crate::wire::ActorObservation {
        actor_id: "a".into(),
        code_sha256: vec![1; 32].into(),
        home_region: String::new(),
        state: ActorState::Active as i32,
        subscriptions: Vec::new(),
        checkpoint_unix_millis,
        checkpoint_epoch,
        configuration_revision,
    }
}

fn assert_observation_projection(
    value: &super::ActorObservation,
    checkpoint_unix_millis: Option<u64>,
    checkpoint_epoch: u64,
    configuration_revision: u64,
) {
    assert_eq!(value.checkpoint_unix_millis(), checkpoint_unix_millis);
    assert_eq!(value.checkpoint_epoch(), checkpoint_epoch);
    assert_eq!(value.configuration_revision(), configuration_revision);
}

fn assert_observation_wire_projection(
    value: &crate::wire::ActorObservation,
    checkpoint_unix_millis: Option<u64>,
    checkpoint_epoch: u64,
    configuration_revision: u64,
) {
    assert_eq!(value.checkpoint_unix_millis, checkpoint_unix_millis);
    assert_eq!(value.checkpoint_epoch, checkpoint_epoch);
    assert_eq!(value.configuration_revision, configuration_revision);
}

#[kani::proof]
#[kani::unwind(40)]
fn actor_observation_and_response_numeric_projection_is_lossless() {
    let millis: Option<u64> = kani::any();
    let epoch: u64 = kani::any();
    let revision: u64 = kani::any();
    let value = super::ActorObservation::try_from(observation(millis, epoch, revision)).unwrap();
    assert_observation_projection(&value, millis, epoch, revision);
    let wire: crate::wire::ActorObservation = value.into();
    assert_observation_wire_projection(&wire, millis, epoch, revision);

    let create_present: bool = kani::any();
    let create = super::CreateActorResponse::try_from(crate::wire::CreateActorResponse {
        actor: create_present.then(|| observation(millis, epoch, revision)),
    })
    .unwrap();
    assert_eq!(create.actor().is_some(), create_present);
    if let Some(actor) = create.actor() {
        assert_observation_projection(actor, millis, epoch, revision);
    }
    let wire: crate::wire::CreateActorResponse = create.into();
    assert_eq!(wire.actor.is_some(), create_present);
    if let Some(actor) = wire.actor {
        assert_observation_wire_projection(&actor, millis, epoch, revision);
    }

    let update_present: bool = kani::any();
    let update = super::UpdateActorResponse::try_from(crate::wire::UpdateActorResponse {
        actor: update_present.then(|| observation(millis, epoch, revision)),
    })
    .unwrap();
    assert_eq!(update.actor().is_some(), update_present);
    if let Some(actor) = update.actor() {
        assert_observation_projection(actor, millis, epoch, revision);
    }
    let wire: crate::wire::UpdateActorResponse = update.into();
    assert_eq!(wire.actor.is_some(), update_present);
    if let Some(actor) = wire.actor {
        assert_observation_wire_projection(&actor, millis, epoch, revision);
    }
}

#[kani::proof]
#[kani::unwind(40)]
fn update_expected_configuration_revision_is_lossless() {
    let revision: u64 = kani::any();
    let value = super::UpdateActorRequest::try_from(crate::wire::UpdateActorRequest {
        actor_id: "a".into(),
        code_sha256: vec![1; 32].into(),
        bindings: Vec::new(),
        limits: Some(crate::wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        }),
        expected_configuration_revision: revision,
        idempotency_key: "i".into(),
    })
    .unwrap();
    assert_eq!(value.expected_configuration_revision(), revision);
    let wire: crate::wire::UpdateActorRequest = value.into();
    assert_eq!(wire.expected_configuration_revision, revision);
}
