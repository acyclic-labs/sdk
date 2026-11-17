#[path = "../../crates/stream/src/preparation/clock.rs"]
mod production;

#[kani::proof]
fn committed_time_is_the_exact_clamped_monotonic_projection() {
    let (previous, sampled): (u64, u64) = kani::any();
    let exact = u128::from(previous).max(u128::from(sampled) * 1_000);
    let actual = production::committed_time(previous, sampled);
    assert_eq!(u128::from(actual), exact.min(u128::from(u64::MAX)));
}
