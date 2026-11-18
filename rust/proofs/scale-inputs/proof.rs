#[path = "../../crates/test-support/src/scale/ordered.rs"]
mod production;

#[kani::proof]
fn scale_inputs_admit_exactly_positive_increasing_pairs() {
    let small: u64 = kani::any();
    let large: u64 = kani::any();
    let inputs = production::OrderedInputs::new(small, large);
    assert_eq!(inputs.is_some(), small > 0 && large > small);
    if let Some(inputs) = inputs {
        assert_eq!(inputs.small(), small);
        assert_eq!(inputs.large(), large);
    }
}
