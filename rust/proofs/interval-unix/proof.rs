#[path = "../../crates/observability-core/src/interval/unix.rs"]
mod production;

#[kani::proof]
fn unix_offsets_admit_exactly_the_representable_affine_coordinates() {
    let observed: u128 = kani::any();
    let window: u128 = kani::any();
    let offset: u128 = kani::any();
    // Independent 129-bit affine equation: carry and borrow must cancel.
    let (sum, carry) = observed.overflowing_add(offset);
    let (coordinate, borrow) = sum.overflowing_sub(window / 2);
    assert_eq!(
        production::project_offset(observed, window, offset),
        if carry == borrow {
            Some(coordinate)
        } else {
            None
        }
    );
}
