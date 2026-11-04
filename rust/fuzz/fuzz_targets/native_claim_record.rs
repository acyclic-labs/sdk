//! Exercise persisted native claim admission through the production codec.
#![no_main]

#[path = "../../crates/native-runtime/tests/claim_codec/oracle.rs"]
mod native_claim_record;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| native_claim_record::check(bytes));
