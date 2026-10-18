#![no_main]

#[path = "../../tests/support/completion_deltas.rs"]
mod deltas;
#[path = "../../tests/support/completion_sequences.rs"]
mod sequences;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    assert!(sequences::exercise(data).is_ok());
    deltas::exercise(data);
});
