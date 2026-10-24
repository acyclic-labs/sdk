//! Fuzz policy, delegation and native path admission with the shared oracle.
#![no_main]
use acyclic_fs::fuzzing::access_input as input;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| input::check(bytes));
