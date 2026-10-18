#![no_main]

mod recorded_invocation_input;
pub use recorded_invocation_input::fuzz_support;
fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| recorded_invocation_input::check(fuzz_input));
