#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| syntect::test_support::check_syntax_data(data));
