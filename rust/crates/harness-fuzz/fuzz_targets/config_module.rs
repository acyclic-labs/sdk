#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    futures::executor::block_on(acyclic_harness::config::model::check(input)).unwrap();
});
