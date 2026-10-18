#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    acyclic_harness_providers::fuzzing::assert_image_header(data);
});
