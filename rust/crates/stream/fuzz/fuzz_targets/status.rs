#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_stream::wire_codec::status_contract::check(input)
        .expect("Stream trailer status contract");
});
