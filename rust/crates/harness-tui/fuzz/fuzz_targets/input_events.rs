#![no_main]

#[path = "../../tests/support/input_sequences.rs"]
mod sequences;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    sequences::exercise(data);
});
