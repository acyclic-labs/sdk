#![cfg(feature = "native-answer")]

#[path = "../native_answer_oracle.rs"]
mod oracle;

#[test]
fn recorded_and_derived_native_events_satisfy_the_same_fuzz_oracle() {
    let corpus = acyclic_test_support::crate_path!("corpus/native_answer");
    let mut inputs: Vec<_> = std::fs::read_dir(corpus)
        .expect("native event corpus")
        .map(|entry| entry.expect("corpus entry").path())
        .collect();
    inputs.sort();
    assert!(!inputs.is_empty(), "native event corpus missing");
    for input in inputs {
        eprintln!("native event input: {}", input.display());
        assert!(
            oracle::check(&std::fs::read(input).expect("native event bytes")) > 0,
            "recorded/derived event input was never admitted"
        );
    }

    // Derived terminal shapes cross many pipe pages and the former frame ceiling.
    // The original captures above retain their separate interoperability provenance.
    let text = "🌍".repeat(33_000);
    for events in [
        vec![
            serde_json::json!({"type": "item.completed", "item": {"type": "agent_message", "text": &text}}),
            serde_json::json!({"type": "turn.completed"}),
        ],
        vec![
            serde_json::json!({"type": "result", "subtype": "success", "is_error": false, "result": &text}),
        ],
        vec![
            serde_json::json!({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop", "content": [{"text": &text}]}}),
        ],
        vec![
            serde_json::json!({"event": "result", "result": {"status": "SUCCESS", "response": &text}}),
        ],
        vec![
            serde_json::json!({"type": "text", "part": {"id": "complete", "text": &text, "time": {"end": 1}}}),
            serde_json::json!({"type": "step_finish", "part": {"reason": "stop"}}),
        ],
    ] {
        let mut bytes = Vec::new();
        for event in &events {
            serde_json::to_writer(&mut bytes, event).expect("derived event encoding");
            bytes.push(b'\n');
        }
        assert_eq!(
            oracle::check(&bytes),
            events.len(),
            "a complete native event was discarded before its terminal"
        );
    }
}
