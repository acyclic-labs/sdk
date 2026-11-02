//! Raw user resolutions enter the same decoder as staged merge proposals.
#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_fs::workspace::check_merge_directory_resolution(bytes);
});
