//! Exercises decoded-cache admission and retention through its shared oracle.
#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    assert!(acyclic_fs::cache::decoded_fuzz::check(input).is_ok());
});
