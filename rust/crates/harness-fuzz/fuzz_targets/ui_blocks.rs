#![no_main]
#[path = "../ui_blocks_oracle.rs"]
mod oracle;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| oracle::check(bytes));
