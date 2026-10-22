#![no_main]
#[cfg(test)]
use acyclic_harness::{
    AgentId, Result, conversation, executor, filesystem, model, projection, resources, tool,
};

mod recorded_invocation_input;
libfuzzer_sys::fuzz_target!(|data: &[u8]| recorded_invocation_input::check(data));
