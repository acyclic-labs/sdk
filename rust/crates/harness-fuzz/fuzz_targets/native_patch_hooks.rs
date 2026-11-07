#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_core::hooks::check_native_patch_paths(input);
});
