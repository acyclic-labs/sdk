//! Direct proofs of nominal admission and wire enum identities.

use super::{
    ActorLimits, ActorState, CodeSha256, DomainError, ErrorCode, PositiveU64, SubscriptionStart,
    SubscriptionState,
};

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
fn current_head_marker_bool_ingress_is_exact() {
    let raw: bool = kani::any();

    match super::subscription_start::CurrentHeadMarker::try_from(raw) {
        Ok(marker) => {
            assert!(raw);
            assert!(bool::from(marker));
        }
        Err(error) => {
            assert!(!raw);
            assert_eq!(error, DomainError::InvalidSubscription);
        }
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn subscription_start_oneof_ingress_preserves_cursor_and_current_head() {
    let cursor: u64 = kani::any();

    match SubscriptionStart::try_from(crate::wire::SubscriptionStart {
        start: Some(crate::wire::subscription_start::Start::Cursor(cursor)),
    }) {
        Ok(value) => {
            assert_eq!(value.cursor_value(), Some(cursor));
            assert_eq!(value.current_head_value(), None);
        }
        Err(_) => {
            assert!(false);
        }
    }

    match SubscriptionStart::try_from(crate::wire::SubscriptionStart {
        start: Some(crate::wire::subscription_start::Start::CurrentHead(true)),
    }) {
        Ok(value) => {
            assert_eq!(value.cursor_value(), None);
            assert_eq!(value.current_head_value(), Some(true));
        }
        Err(_) => {
            assert!(false);
        }
    }

    assert_eq!(
        SubscriptionStart::try_from(crate::wire::SubscriptionStart {
            start: Some(crate::wire::subscription_start::Start::CurrentHead(false)),
        }),
        Err(DomainError::InvalidSubscription)
    );
    assert_eq!(
        SubscriptionStart::try_from(crate::wire::SubscriptionStart { start: None }),
        Err(DomainError::InvalidSubscription)
    );

    let direct_cursor = SubscriptionStart::cursor(cursor);
    assert_eq!(direct_cursor.cursor_value(), Some(cursor));
    assert_eq!(direct_cursor.current_head_value(), None);

    let direct_head = SubscriptionStart::current_head();
    assert_eq!(direct_head.cursor_value(), None);
    assert_eq!(direct_head.current_head_value(), Some(true));
}

// Scope: this harness quantifies exactly every [u8; 32] array whose bytes are
// not all zero. It calls the production CodeSha256::new and proves the stored
// bytes are preserved. It does not quantify arbitrary Vec<u8> lengths.
#[kani::proof]
#[kani::unwind(40)]
fn code_sha256_exact_32_symbolic_bytes_preserved() {
    let value: [u8; 32] = kani::any();
    let mut has_nonzero = false;
    for byte in value {
        has_nonzero |= byte != 0;
    }
    kani::assume(has_nonzero);

    match CodeSha256::new(value.to_vec()) {
        Ok(digest) => assert_eq!(digest.as_bytes(), &value),
        Err(_) => {
            assert!(false);
        }
    }
}

// These are concrete rejection examples for lengths 0, 31, and 33, plus an
// all-zero 32-byte value. They are not a universal theorem over Vec lengths;
// that arbitrary-length property remains a gap until a reusable production
// helper exposes the length domain directly to the proof.
#[kani::proof]
#[kani::unwind(40)]
fn code_sha256_rejects_wrong_lengths_and_all_zero() {
    assert_eq!(
        CodeSha256::new(Vec::new()),
        Err(DomainError::InvalidCodeSha256)
    );
    assert_eq!(
        CodeSha256::new(vec![1; 31]),
        Err(DomainError::InvalidCodeSha256)
    );
    assert_eq!(
        CodeSha256::new(vec![1; 33]),
        Err(DomainError::InvalidCodeSha256)
    );
    assert_eq!(
        CodeSha256::new(vec![0; 32]),
        Err(DomainError::InvalidCodeSha256)
    );
}
