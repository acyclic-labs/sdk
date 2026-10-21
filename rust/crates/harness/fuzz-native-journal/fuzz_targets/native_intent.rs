#![no_main]
acyclic_test_support::counting_allocator!();

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if input.len() > 65_536 {
        return;
    }
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 64_000_000),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    let baseline = work.live_allocation_bytes();
    let measured = acyclic_test_support::measure(|| {
        let _ = acyclic_harness::filesystem::fuzz_native_preparation_intent(input, &work);
    });
    assert!(measured.bytes_max as u64 <= work.receipt().peak_allocation_bytes);
    assert_eq!(work.live_allocation_bytes(), baseline);
});
