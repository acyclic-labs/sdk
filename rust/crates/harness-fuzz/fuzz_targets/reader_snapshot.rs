#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_fuzz::reader_snapshot::check(input).expect("Reader persisted-input contract");
});
