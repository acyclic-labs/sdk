#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    futures::executor::block_on(acyclic_actors::call::model::check(bytes)).unwrap();
});
