#![no_main]

use acyclic_harness::test_support::fuzz_cases::goal_parser_input;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    assert!(goal_parser_input::check(data).is_ok());
});
