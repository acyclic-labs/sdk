#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_codec_fuzz::exercise("machine_journal", bytes).unwrap();
});
