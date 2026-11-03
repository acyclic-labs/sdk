#![no_main]

#[path = "../native_claim_record.rs"]
mod native_claim_record;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| native_claim_record::check(bytes));
