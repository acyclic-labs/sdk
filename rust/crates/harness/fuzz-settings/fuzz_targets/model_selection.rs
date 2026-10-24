#![no_main]
libfuzzer_sys::fuzz_target!(
    |input: &[u8]| acyclic_harness::native_settings::fuzz_model_selection(input)
);
