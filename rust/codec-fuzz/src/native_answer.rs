//! The shipped native answer state machine, compiled from its original source.
use acyclic_harness_core::Outcome;
use serde_json::Value;

#[path = "../../crates/harness/src/local/host_process/children/answer.rs"]
mod answer;

fn replay(bytes: &[u8], chunk: usize) -> answer::NativeAnswer {
    let mut lines = acyclic_native_runtime::lines::LineBuffer::new(None);
    let mut answer = answer::NativeAnswer::default();
    for bytes in bytes.chunks(chunk) {
        for line in lines.push(bytes) {
            let Ok(line) = line else {
                answer.omit();
                continue;
            };
            if let Ok(event) = serde_json::from_str::<Value>(&line) {
                answer.record(&event);
            }
        }
    }
    answer
}

pub fn check(bytes: &[u8]) {
    let whole = replay(bytes, bytes.len().max(1));
    let fragmented = replay(bytes, bytes.first().copied().map_or(1, usize::from).max(1));
    assert_eq!(whole.pending, fragmented.pending);
    assert_eq!(whole.outcome, fragmented.outcome);
    assert_eq!(whole.omitted, fragmented.omitted);
}
