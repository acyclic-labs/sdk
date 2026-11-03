#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_stream::codec_fuzz::status::check(input).expect("Stream trailer status contract");
});
