#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    futures::executor::block_on(acyclic_harness_core::executor::check_code_mode(input));
});
