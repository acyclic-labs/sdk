#![no_main]
#[cfg(test)]
use acyclic_harness::{
    AgentId, HeapLayout, Result, contract, conversation, executor, filesystem, heap_layout, model,
    projection, resources, tool,
};

mod recorded_invocation_input;
libfuzzer_sys::fuzz_target!(|data: &[u8]| recorded_invocation_input::check(data));
