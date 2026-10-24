#![no_main]
mod file_guidance_input;
libfuzzer_sys::fuzz_target!(|input: &[u8]| file_guidance_input::check(input));
