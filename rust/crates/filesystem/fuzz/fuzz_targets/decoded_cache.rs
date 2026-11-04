#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_fs::cache::decoded_fuzz::check(input);
});
