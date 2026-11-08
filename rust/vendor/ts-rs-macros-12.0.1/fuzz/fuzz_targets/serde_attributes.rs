#![no_main]

libfuzzer_sys::fuzz_target!(|source: &str| {
    ts_rs_attribute_fuzz::exercise(source);
});
