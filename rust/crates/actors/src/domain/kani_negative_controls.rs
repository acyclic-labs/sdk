use super::{PositiveU64, SubscriptionState};

#[kani::proof]
#[kani::unwind(1)]
fn positive_u64_negative_control_accepts_zero() {
    assert!(PositiveU64::new(0).is_ok());
}

#[kani::proof]
#[kani::unwind(1)]
fn enum_negative_control_accepts_unknown() {
    assert!(SubscriptionState::try_from(i32::MAX).is_ok());
}
