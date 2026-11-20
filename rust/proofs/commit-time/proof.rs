#![no_std]

include!("../../crates/stream-contract-core/src/preparation/clock.rs");

#[kani::proof]
fn committed_time_is_the_exact_clamped_monotonic_projection() {
    let (prev, ms): (u64, u64) = kani::any();
    let expected = ms.checked_mul(1_000).map_or(u64::MAX, |now| now.max(prev));
    kani::assert(committed_time(prev, ms) == expected, "clamped commit clock");
}
