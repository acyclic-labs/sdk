#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    acyclic_harness::test_support::fuzz_memory_directory(data);
});
