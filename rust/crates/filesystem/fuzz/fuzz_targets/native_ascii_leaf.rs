//! Exact native component validation, encoding and examined-prefix accounting.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: [u8; 88]| {
    acyclic_fs::native_host::check_prepared_ascii_leaf(data);
});
