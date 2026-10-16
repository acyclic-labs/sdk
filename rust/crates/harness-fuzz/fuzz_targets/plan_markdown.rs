#![no_main]

use acyclic_harness::client::parse_task_list;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    if let Some(list) = parse_task_list(text) {
        for item in list.items() {
            assert!(!item.text.trim().is_empty());
        }
    }
});
