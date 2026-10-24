#![no_main]

use acyclic_harness::test_support::fuzz_cases::file_client as client;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| client::exercise(bytes));
