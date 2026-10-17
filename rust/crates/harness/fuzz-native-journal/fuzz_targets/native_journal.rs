#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/filesystem/native_execution/journal_allocation.rs"]
mod journal_allocation;
#[path = "../../src/filesystem/native_execution/journal_encoding.rs"]
mod journal_encoding;
#[path = "../../src/filesystem/native_execution/journal_verification.rs"]
mod journal_verification;

fuzz_target!(|input: &[u8]| {
    if input.len() <= 65_536 {
        let accepted = journal_verification::verify_decode(input);
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
