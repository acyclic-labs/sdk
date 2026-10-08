//! Direct proofs of nominal admission and wire enum identities.

use super::{ActorLimits, ActorState, DomainError, ErrorCode, PositiveU64, SubscriptionStart, SubscriptionState};

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
