#![no_main]

use libfuzzer_sys::fuzz_target;
mod goal_parser_input;
mod goal_scenario_input;
#[path = "../../harness/tests/e2e/goals.rs"]
mod goals;

fuzz_target!(|data: &[u8]| goal_scenario_input::check(data));
