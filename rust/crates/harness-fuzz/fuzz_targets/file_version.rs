#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness::tool::files::check_version(input);
});
