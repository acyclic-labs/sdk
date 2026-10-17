#![no_main]

mod recorded_invocation_input;
libfuzzer_sys::fuzz_target!(|data: &[u8]| recorded_invocation_input::check(data));
