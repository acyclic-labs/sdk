#![no_main]
use acyclic_harness::test_support::fuzz_cases::client_offline as oracle;
libfuzzer_sys::fuzz_target!(|input: &[u8]| oracle::exercise(
    input,
    acyclic_harness::client::validate_offline_request
));
