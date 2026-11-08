//! Exercise editor operations against grapheme, history and undo invariants.
#![no_main]

#[path = "../../tests/support/sequences.rs"]
mod sequences;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    sequences::exercise(data);
});
