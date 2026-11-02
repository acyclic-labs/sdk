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
    assert_eq!(
        session(
            input,
            input.first().map_or(1, |byte| usize::from(*byte) + 1)
        ),
        session(input, 8192),
    );
    // Original failed-tool traces end in successful parent terminals. They
    // must remain failures through the same decoder under either read shape.
    if input == include_bytes!("../corpus/native_reply/claude-tool-refusal")
        || input == include_bytes!("../corpus/native_reply/codex-tool-refusal")
    {
        assert!(matches!(pages.outcome, Some(Outcome::Failed { .. })));
    }
    if input == include_bytes!("../corpus/native_reply/claude-code-completed.jsonl") {
        // Derived JSON representation, with the captured member/value unchanged.
        // The production JSON decoder owns escapes, including in member names.
        let captured = std::str::from_utf8(input).expect("recorded Claude terminal");
        let escaped = captured.replace("\"session_id\"", r#""sess\u0069on\u005fid""#);
        assert_eq!(session(escaped.as_bytes(), 1), session(input, 8192),);
    }
});

// Exercise the production closed-log resume fold as well as the live reply
// fold. Its EOF delimiter is parser-only and never changes artifact bytes.
fn session(input: &[u8], chunk_size: usize) -> Option<String> {
    let mut session = answer::SessionId::default();
    for bytes in input.chunks(chunk_size) {
        session.push(bytes);
    }
    session.finish();
    if let Some(id) = &session.id {
        assert!(!id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control));
    }
    session.id
}

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
        match &reply.outcome {
            Some(Outcome::Succeeded(text) | Outcome::Failed { message: text }) => {
                assert!(text.len() <= answer::ANSWER_LIMIT);
            }
            _ => {}
        }
    }
    reply
}
