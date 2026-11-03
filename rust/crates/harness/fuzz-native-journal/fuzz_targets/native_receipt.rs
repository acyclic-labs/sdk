#![no_main]

#[path = "../../tests/support/native_receipt.rs"]
mod contract;

libfuzzer_sys::fuzz_target!(|input: &[u8]| contract::verify(input));
