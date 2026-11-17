#[path = "../../crates/stream/src/preparation/clock.rs"]
mod production;

#[kani::proof]
fn committed_time_is_the_exact_clamped_monotonic_projection() {
    let (previous, sampled): (u64, u64) = kani::any();
    let expected = if sampled > u64::MAX / 1_000 {
        u64::MAX
    } else if sampled * 1_000 > previous {
        sampled * 1_000
    } else {
        previous
    };
    assert_eq!(production::committed_time(previous, sampled), expected);
}
