#![no_main]

#[path = "../../harness-contracts/tests/support/wire_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    input::check(data);
});
