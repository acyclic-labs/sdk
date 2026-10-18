#![no_main]

pub use goal_parser_input::fuzz_support;
mod goal_parser_input;

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    assert!(goal_parser_input::check(fuzz_input).is_ok());
});
