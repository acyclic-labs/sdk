//! Fuzz the shipped versioned signing-reference codec and custody binding.
#![no_main]
#[path = "../../crates/native-runtime/tests/signing_reference/oracle.rs"]
mod oracle;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| oracle::check(bytes));
