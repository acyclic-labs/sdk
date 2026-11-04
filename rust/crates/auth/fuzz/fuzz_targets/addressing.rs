#![no_main]
use acyclic_auth::addressing::testing;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| testing::check(data));
