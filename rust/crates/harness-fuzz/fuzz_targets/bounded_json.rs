#![no_main]
#[path = "../../harness/tests/support/bounded_json.rs"]
mod case;
libfuzzer_sys::fuzz_target!(|input: &[u8]| case::check(input));
