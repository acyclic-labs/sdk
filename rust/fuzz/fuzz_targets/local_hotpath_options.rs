#![no_main]

#[path = "../local_hotpath_options.rs"]
mod local_hotpath_options;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    local_hotpath_options::check(input);
});
