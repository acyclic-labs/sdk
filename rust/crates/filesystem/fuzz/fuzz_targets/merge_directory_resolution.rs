//! Raw resolutions and persisted merge files enter their shipped decoders.
#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_fs::workspace::check_merge_directory_resolution(bytes);
});
