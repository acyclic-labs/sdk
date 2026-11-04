#![no_main]
//! The same original follower and corpus oracle runs in default unit builds.
use libfuzzer_sys::fuzz_target;

#[path = "../../tests/common/trust_record.rs"]
mod trust_record_check;

fuzz_target!(|bytes: &[u8]| trust_record_check::check(bytes));
