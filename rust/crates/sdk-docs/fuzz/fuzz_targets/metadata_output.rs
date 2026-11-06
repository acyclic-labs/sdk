#![no_main]
use sdk_docs as child;
#[path = "../metadata_output.rs"]
mod metadata_output_oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| metadata_output_oracle::check(bytes));
