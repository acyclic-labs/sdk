#![no_main]

#[path = "../../tests/support/streaming_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| input::check(data));
