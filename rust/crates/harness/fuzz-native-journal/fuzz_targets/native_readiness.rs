//! Check native ready-queue schedules against production registrations.
#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness::test_support::check_native_readiness(input);
});
