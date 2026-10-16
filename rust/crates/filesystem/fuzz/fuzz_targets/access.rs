//! Fuzz policy, delegation and native path admission with the shared oracle.
#![no_main]
#[path = "../../tests/access_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| input::check(bytes));
