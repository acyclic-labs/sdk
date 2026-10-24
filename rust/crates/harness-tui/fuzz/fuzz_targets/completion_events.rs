#![no_main]

use acyclic_harness_tui::fuzzing::completion_deltas as deltas;
use acyclic_harness_tui::fuzzing::completion_sequences as sequences;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    assert!(sequences::exercise(data).is_ok());
    deltas::exercise(data);
});
