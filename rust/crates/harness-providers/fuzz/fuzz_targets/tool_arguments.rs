//! Exercises production tool argument admission with untrusted input.
#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness_providers::fuzzing::assert_tool_arguments(input);
});
