#![no_main]
#[path = "../../harness/tests/e2e/file_bytes.rs"]
mod file_bytes;
#[path = "../../harness/tests/e2e/input_files.rs"]
mod input_files;
mod input_files_input;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| input_files_input::check(bytes));
