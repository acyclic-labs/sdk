#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::client::parse_task_list;

fuzz_support::fuzz_target!(|text: &str, fuzz_input| {
    if let Some(list) = fuzz_support::FuzzInput::optional_text(fuzz_input, parse_task_list) {
        for item in list.items() {
            assert!(!item.text.trim().is_empty());
        }
    }
});
