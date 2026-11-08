#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_core::hooks::codec_fuzz::check(bytes).expect("hook codec invariants");
});
