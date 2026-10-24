#![no_main]
#[cfg(test)]
use acyclic_harness::{
    AgentId, Result, contract, conversation, executor, filesystem, model, projection, resources,
    tool,
};

use acyclic_harness::test_support::fuzz_cases::recorded_invocation_input;
libfuzzer_sys::fuzz_target!(|data: &[u8]| recorded_invocation_input::check(data));
