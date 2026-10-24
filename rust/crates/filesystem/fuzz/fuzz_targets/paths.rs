//! Exercise portable path normalization and rejection.
#![no_main]

use acyclic_fs::fuzzing::path_input as input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() <= 4096 {
        input::check(data);
    }
});
