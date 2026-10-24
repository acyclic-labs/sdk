#![no_main]

use acyclic_harness_tui::fuzzing::sequences;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    sequences::exercise(data);
});
