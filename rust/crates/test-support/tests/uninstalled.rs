//! Negative control: a binary that never installs the counting allocator
//! must not read every closure as allocation-free.

#[test]
#[should_panic(expected = "needs `acyclic_test_support::counting_allocator!();`")]
fn measure_refuses_a_binary_without_the_counting_allocator() {
    let _ = acyclic_test_support::measure(|| drop(std::hint::black_box(Box::new(1u8))));
}
