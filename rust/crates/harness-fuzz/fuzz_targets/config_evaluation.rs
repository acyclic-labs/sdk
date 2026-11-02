#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    futures::executor::block_on(acyclic_harness_core::config::model::check(bytes)).unwrap();
});
