#![no_main]
acyclic_test_support::counting_allocator!();

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if input.len() <= 65_536 {
        let accepted =
            acyclic_harness::filesystem::verify_native_preparation_journal(input, |decode| {
                acyclic_test_support::measure(decode).bytes_max as u64
            })
            .expect("shared journal verification must succeed");
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 64_000_000),
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        let mut decoded = Ok(());
        let measured = acyclic_test_support::measure(|| {
            decoded = acyclic_harness::filesystem::fuzz_native_preparation_journal(input, &work);
        });
        assert!(measured.bytes_max as u64 <= work.receipt().peak_allocation_bytes);
        if accepted {
            assert!(decoded.is_ok());
        }
        drop(decoded);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
});
