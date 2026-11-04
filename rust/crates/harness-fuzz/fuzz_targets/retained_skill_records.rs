#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_core::context::native_skills::codec_fuzz::check(bytes);
});
