#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_native_settings::hooks_codec_fuzz::check(bytes).expect("hook codec invariants");
});
