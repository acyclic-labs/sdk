#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    acyclic_harness::test_support::check_scheduler_intent_frame(bytes)
        .expect("scheduler intent projections and roundtrip");
});
