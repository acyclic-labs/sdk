#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_core::tools::check_agent_wait_selection(bytes);
});
