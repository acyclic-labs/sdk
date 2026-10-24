#![no_main]
use acyclic_harness::test_support::fuzz_cases::queries;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    queries::exercise(bytes);
});
