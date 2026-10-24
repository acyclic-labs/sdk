#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| acyclic_harness::local::fuzz_file_guidance(input));
