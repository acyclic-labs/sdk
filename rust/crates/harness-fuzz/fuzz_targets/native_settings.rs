#![no_main]

use acyclic_harness_core as harness;
#[path = "../../harness-core/src/native_settings/validation/settings.rs"]
mod contract;
#[path = "../../harness-core/src/native_settings/model_selection.rs"]
mod selection;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| contract::exercise(bytes));
