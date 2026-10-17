#![no_main]
use libfuzzer_sys::fuzz_target;
#[path = "../../tests/support/config_sequences.rs"]
mod sequences;
fuzz_target!(|bytes: &[u8]| sequences::exercise(acyclic_harness_tui::config::ConfigFile::Settings, bytes));
