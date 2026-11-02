#![no_main]

use acyclic_harness::Outcome;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

#[path = "../../src/local/host_process/children/answer.rs"]
mod answer;

fuzz_target!(|input: &[u8]| {
    let mut reply = answer::NativeAnswer::default();
    for bytes in input.split_inclusive(|byte| *byte == b'\n') {
        let previous = reply.outcome.clone();
        let refused = matches!(
            previous,
            Some(Outcome::Failed { .. } | Outcome::Cancelled | Outcome::Indeterminate { .. })
        );
        let line = String::from_utf8(bytes.to_vec())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
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
});
