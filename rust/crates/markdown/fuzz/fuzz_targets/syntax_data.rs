#![no_main]

#[path = "../../tests/support/syntax_data.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| input::check(data));
