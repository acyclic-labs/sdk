#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_providers::fuzzing::assert_http_failure_invariants(bytes);
});
