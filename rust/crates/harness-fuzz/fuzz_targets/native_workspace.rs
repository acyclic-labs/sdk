#![no_main]

#[path = "../../harness-core/src/native_settings/validation/workspace.rs"]
mod contract;
#[path = "../../harness-core/src/native_settings/workspace.rs"]
mod workspace;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| contract::exercise(bytes));
