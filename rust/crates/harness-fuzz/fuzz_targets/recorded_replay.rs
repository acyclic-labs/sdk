#![no_main]

libfuzzer_sys::fuzz_target!(|schedule: &[u8]| {
    acyclic_harness_core::test_support::assert_replay_admission(schedule);
});
