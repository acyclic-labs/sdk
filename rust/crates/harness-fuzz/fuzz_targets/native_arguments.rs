#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let observed = acyclic_harness_core::tools::native::check_native_arguments(input);
    assert!(
        observed.is_ok(),
        "native accounting observation failed: {observed:?}"
    );
});
