#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_core::native_settings::test_support::check_model_selection(bytes);
});
