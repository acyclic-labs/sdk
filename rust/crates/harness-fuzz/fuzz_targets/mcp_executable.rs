#![no_main]

libfuzzer_sys::fuzz_target!(|input: &str| {
    acyclic_harness::mcp::fuzz_executable(input);
});
