#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_mcp::settings_validation::check_settings(bytes);
});
