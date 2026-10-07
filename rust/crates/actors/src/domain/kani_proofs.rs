//! Small, pure Kani harnesses for Rust-owned Actors invariants.
//!
//! This module is compiled only by `cargo kani`. It is a child of `domain`,
//! so it can call the existing crate-private predicate without changing its
//! visibility or creating a second validation rule.

use super::{valid_code_sha256, ActorState, DomainError, ErrorCode, SubscriptionState};

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
        _ => assert!(false),
    }
}

fn assert_actor_state_mapping(raw: i32) {
    match ActorState::try_from(raw) {
        Ok(value) => assert_eq!(i32::from(value), raw),
        Err(DomainError::UnknownActorState(found)) => assert_eq!(found, raw),
        _ => assert!(false),
    }
}

fn assert_error_code_mapping(raw: i32) {
    match ErrorCode::try_from(raw) {
        Ok(value) => assert_eq!(i32::from(value), raw),
        Err(DomainError::UnknownErrorCode(found)) => assert_eq!(found, raw),
        _ => assert!(false),
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn enum_numeric_mappings_are_inverse_and_lossless() {
    let subscription_raw: i32 = kani::any();
    assert_subscription_state_mapping(subscription_raw);

    let actor_raw: i32 = kani::any();
    assert_actor_state_mapping(actor_raw);

