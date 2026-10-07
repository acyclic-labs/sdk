//! Small, pure Kani harnesses for Rust-owned Actors invariants.
//!
//! This module is compiled only by `cargo kani`. It is a child of `domain`,
//! so it can call the existing crate-private predicate without changing its
//! visibility or creating a second validation rule.

use super::{
    ActorLimits, ActorState, CodeSha256, DomainError, ErrorCode, PositiveU64, SubscriptionStart,
    SubscriptionState, valid_code_sha256, wire,
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
