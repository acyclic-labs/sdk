#![no_main]
use acyclic_harness_tui::fuzzing::config_sequences as sequences;
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| sequences::exercise(
    acyclic_harness_tui::config::ConfigFile::Keymap,
    bytes
));
