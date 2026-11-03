#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_codec_fuzz::agent_controls::check(bytes);
});
