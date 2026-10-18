#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_providers::fuzzing::assert_catalog_invariants(input);
});
