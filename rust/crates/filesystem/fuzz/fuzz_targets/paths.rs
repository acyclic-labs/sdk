//! Exercise portable path normalization and rejection.
#![no_main]

#[path = "../../tests/path_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() <= 4096 {
        input::check(data);
    }
});
