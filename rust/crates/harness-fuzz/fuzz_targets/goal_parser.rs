#![no_main]

use libfuzzer_sys::fuzz_target;
mod goal_parser_input;

fuzz_target!(|data: &[u8]| {
    assert!(goal_parser_input::check(data).is_ok());
});
