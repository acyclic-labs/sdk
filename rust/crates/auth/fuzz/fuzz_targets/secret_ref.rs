#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| acyclic_auth::secrets::testing::check(bytes));
