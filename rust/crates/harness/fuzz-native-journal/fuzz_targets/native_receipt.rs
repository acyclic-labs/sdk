#![no_main]

#[path = "../oracles/native_receipt.rs"]
mod contract;

libfuzzer_sys::fuzz_target!(|input: &[u8]| contract::verify(input));
