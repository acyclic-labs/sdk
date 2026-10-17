#![no_main]
#[path = "../../harness/tests/support/input_path_sequences.rs"]
mod sequences;
libfuzzer_sys::fuzz_target!(|data: &[u8]| sequences::exercise(data));
