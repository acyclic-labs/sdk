//! JSON representation invariance at the shipping native recovery selector.
#![no_main]

use acyclic_harness::local::host_owner::logged_session_id;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fuzz_target!(|bytes: &[u8]| {
    // The unchanged native capture keeps the acceptance branch reachable;
    // refusing every resume id cannot satisfy a metamorphic oracle alone.
    let original =
        include_bytes!("../../harness/src/local/host_process/fixtures/claude-code-completed.jsonl");
    assert_eq!(
        logged_session_id(original).as_deref(),
        Some("a14f2590-4397-48a1-a076-1f2dac2d5349")
    );
    let opencode = include_bytes!("../corpus/native_answer/opencode-incomplete.jsonl");
    assert_eq!(
        logged_session_id(opencode).as_deref(),
        Some("ses_e4b5a1b9fffeksLehNjEEeDfZ2")
    );
    let agy = include_bytes!("../corpus/native_answer/agy-authentication-error.jsonl");
    assert!(
        logged_session_id(agy).is_none(),
        "empty native conversation id admitted"
    );
    let agy_refusal = include_bytes!("../corpus/native_answer/agy-hook-refusal.jsonl");
    assert_eq!(
        logged_session_id(agy_refusal).as_deref(),
        Some("b12c0707-3dcb-46c3-a39c-b1fee257dca0")
    );
    for log in [
        original.as_slice(),
        opencode.as_slice(),
        agy.as_slice(),
        agy_refusal.as_slice(),
        bytes,
    ] {
        // Ignore malformed lines exactly as the closed-log boundary does.
        // Canonicalisation and equivalent escaped members/types must select
        // the same original id; this does not reimplement its selection rules.
        let canonical = log
            .split(|byte| *byte == b'\n')
            .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
            .map(|event| serde_json::to_string(&event).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        let escaped = canonical
            .replace("thread_id", r"thread\u005fid")
            .replace("session_id", r"sess\u0069on\u005fid")
            .replace("sessionID", r"sess\u0069onID")
            .replace("conversation_id", r"conversat\u0069on_id")
            .replace(r#""session""#, r#""sess\u0069on""#);
        let decoded = |text: &str| {
            text.lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(decoded(&canonical), decoded(&escaped));
        assert_eq!(
            logged_session_id(canonical.as_bytes()),
            logged_session_id(log)
        );
        assert_eq!(
            logged_session_id(escaped.as_bytes()),
            logged_session_id(log)
        );
    }
});
