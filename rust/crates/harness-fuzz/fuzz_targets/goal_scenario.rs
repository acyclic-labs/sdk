#![no_main]

use acyclic_harness::test_support::fuzz_cases::goal_scenario_input;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| goal_scenario_input::check(data));
