#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    futures::executor::block_on(acyclic_stream::conformance::selector_model::check(bytes)).unwrap();
});
