//! Replay original native output through the shared production reply oracle.
#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness::test_support::native_answer::check(input);
});
