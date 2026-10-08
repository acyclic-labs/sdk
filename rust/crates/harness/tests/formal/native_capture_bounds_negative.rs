//! Deliberately false assertions: these solver runs must find counterexamples.

#[path = "../../src/filesystem/native_capture_bounds.rs"]
mod production;

#[kani::proof]
fn zero_capture_timeout_is_accepted() {
    assert!(production::capture_allowances_valid(0, 1, 1, 1, 2052));
}

#[kani::proof]
fn maximum_output_fits_small_result() {
    assert!(production::capture_allowances_valid(1, 1, 1, u32::MAX, u32::MAX));
}

fn main() {}
