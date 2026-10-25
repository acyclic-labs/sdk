use acyclic_test_support::{AllocationInfo, measure};

acyclic_test_support::counting_allocator!();

#[inline(always)]
fn retire_scalar(value: u64) -> AllocationInfo {
    let value = Box::new(value);
    measure(move || drop(value))
}

#[test]
fn captured_storage_crosses_the_measurement_boundary() {
    let observed = retire_scalar(0);
    assert_eq!(observed.count_current, -1);
    assert_eq!(observed.bytes_current, -8);
    assert_eq!(observed.count_total, 0);
}
