#![no_main]

#[path = "../../harness-core/src/native_settings/validation/model.rs"]
mod contract;
#[path = "../../harness-core/src/native_settings/model_selection.rs"]
mod selection;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| contract::exercise(bytes));
