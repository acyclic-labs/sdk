#![no_main]

use acyclic_harness::Outcome;
use acyclic_native_runtime::lines::LineBuffer;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

#[path = "../../src/local/host_process/children/answer.rs"]
mod answer;

fuzz_target!(|input: &[u8]| {
    // Both executions use the deployed framer and byte bound. Vary only the
    // pipe's read boundaries, retaining every original input byte.
    let fragmented = fold(
        input,
        input.first().map_or(1, |byte| usize::from(*byte) + 1),
    );
    let pages = fold(input, 8192);
    assert_eq!(fragmented.pending, pages.pending);
    assert_eq!(fragmented.outcome, pages.outcome);
    assert_eq!(fragmented.omitted, pages.omitted);
});

fn fold(input: &[u8], chunk_size: usize) -> answer::NativeAnswer {
    let mut frames = LineBuffer::new(answer::EVENT_LIMIT);
    let mut reply = answer::NativeAnswer::default();
    for line in input
        .chunks(chunk_size)
        .flat_map(|bytes| frames.push(bytes))
    {
        let previous = reply.outcome.clone();
        let refused = matches!(
            previous,
            Some(Outcome::Failed { .. } | Outcome::Cancelled | Outcome::Indeterminate { .. })
        );
        if let Some((_, event)) = answer::decode(line) {
            reply.record(&event);
            if refused && event.get("type").and_then(Value::as_str) != Some("turn.started") {
                assert_eq!(reply.outcome, previous);
            }
        } else {
            reply.omit();
            assert!(reply.pending.is_none());
            assert!(!matches!(reply.outcome, Some(Outcome::Succeeded(_))));
            if refused {
                assert_eq!(reply.outcome, previous);
            }
        }
        if let Some(text) = &reply.pending {
            assert!(text.len() <= answer::ANSWER_LIMIT);
        }
        if let Some(Outcome::Succeeded(text)) = &reply.outcome {
            assert!(text.len() <= answer::ANSWER_LIMIT);
        }
    }
    reply
}
