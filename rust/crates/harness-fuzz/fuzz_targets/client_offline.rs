#![no_main]
#[path = "../support/client_offline.rs"]
mod oracle;
libfuzzer_sys::fuzz_target!(|input: &[u8]| oracle::exercise(
    input,
    acyclic_harness::client::validate_offline_request
));
