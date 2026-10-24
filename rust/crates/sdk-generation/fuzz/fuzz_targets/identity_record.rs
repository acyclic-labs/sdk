#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| sdk_generation::fuzz_identity_record(bytes));
