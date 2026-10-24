#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| sdk_docs::fuzz_rustdoc_adapter(bytes));
