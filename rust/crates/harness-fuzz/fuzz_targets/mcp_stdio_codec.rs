#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    acyclic_harness::mcp::stdio::fuzz_codec(input);
});
