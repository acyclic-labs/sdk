use super::super::native_reasoning::replay_responses_with;
use crate::fuzzing::{assert_response_sequence, response_repair_violation};
use proptest::prelude::*;
use serde_json::{Value, json};

fn item(kind: u8, text: &str) -> Value {
    match kind % 6 {
        0 => json!({"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"native"}),
        1 => json!({"role":"assistant","content":text}),
        2 => json!({"type":"function_call","call_id":"call_1","name":"echo","arguments":"{}"}),
        3 => json!({"role":"user","content":text}),
        4 => json!({"type":"function_call_output","call_id":"call_1","output":text}),
        // Stored reasoning without a native envelope is still a reasoning position.
        _ => json!({"type":"reasoning","id":"stored","summary":[]}),
    }
}

fn body(kinds: &[u8], text: &str) -> Vec<u8> {
    let items: Vec<_> = kinds.iter().map(|kind| item(*kind, text)).collect();
    serde_json::to_vec(&json!({"store":false,"input":items})).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn structured_sequences_are_repaired_minimally(
        kinds in prop::collection::vec(0u8..6, 0..24), text in ".{0,48}",
    ) {
        assert_response_sequence(&body(&kinds, &text));
    }

    #[test]
    fn arbitrary_bytes_keep_the_oracle_total(input in prop::collection::vec(any::<u8>(), 0..512)) {
        assert_response_sequence(&input);
    }
}

#[test]
fn negative_controls_fault_the_production_successor_rule() {
    for successor in [0, 3, 4, 5] {
        let items = [item(0, ""), item(successor, "interrupt")];
        assert_response_sequence(&body(&[0, successor], "interrupt"));
        let never = replay_responses_with(&items, |_, _| true);
        assert_eq!(
            response_repair_violation(&items, &never),
            Some("unanswered reasoning")
        );
    }
    let answered = [item(0, ""), item(1, "answer")];
    let always = replay_responses_with(&answered, |_, _| false);
    assert_eq!(
        response_repair_violation(&answered, &always),
        Some("unrequired repair")
    );
}

#[test]
fn checked_in_byte_corpus() {
    for bytes in [
        include_bytes!("../../../fuzz/corpus/response_sequence/successors.json").as_slice(),
        include_bytes!("../../../fuzz/corpus/response_sequence/untyped-assistant.json").as_slice(),
        include_bytes!("../../../fuzz/corpus/response_sequence/malformed-skipped.json").as_slice(),
    ] {
        assert_response_sequence(bytes);
    }
    // A lone surrogate in an ignored field is rejected before validation.
    assert!(
        crate::json(include_bytes!(
            "../../../fuzz/corpus/response_sequence/malformed-skipped.json"
        ))
        .is_err()
    );
}
