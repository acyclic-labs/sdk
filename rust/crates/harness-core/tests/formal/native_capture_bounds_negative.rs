//! Deliberately false assertions: each harness verifies only when the solver finds its counterexample.

#[path = "../../src/filesystem/native_capture_bounds.rs"]
mod production;

#[kani::proof]
#[kani::should_panic]
fn zero_capture_timeout_is_accepted() {
    assert!(production::capture_allowances_valid(0, 1, 1, 2052));
}

#[kani::proof]
#[kani::should_panic]
fn maximum_output_fits_small_result() {
    assert!(production::capture_allowances_valid(
        1,
        1,
        u32::MAX,
        u32::MAX
    ));
}

fn main() {}
