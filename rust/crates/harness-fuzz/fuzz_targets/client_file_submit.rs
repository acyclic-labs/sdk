#![no_main]

#[path = "../support/file_client.rs"]
mod client;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| client::exercise(bytes));
