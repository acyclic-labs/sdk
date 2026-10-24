#![no_main]

use acyclic_markdown::fuzzing::streaming_input as input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| input::check(data));
