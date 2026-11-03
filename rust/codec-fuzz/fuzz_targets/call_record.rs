#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_codec_fuzz::exercise("call_record", bytes)
        .expect("original call protocol must conserve its committed receipts");
});
