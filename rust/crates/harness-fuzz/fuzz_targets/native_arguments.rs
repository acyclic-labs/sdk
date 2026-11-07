#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_core::tools::native::check_native_arguments(input);
});
