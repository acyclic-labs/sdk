#![no_main]
libfuzzer_sys::fuzz_target!(
    |input: &str| acyclic_harness::native_settings::fuzz_workspace_key(input)
);
