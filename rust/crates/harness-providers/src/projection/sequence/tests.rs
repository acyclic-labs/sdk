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

mod turns {
    use super::super::super::wire::Item;
    use super::super::super::wire::replay_turns_with;
    use super::super::{RULES, Rules};
    use crate::Adapter;
    use crate::fuzzing::{assert_turns, canonical_turns, turn_repair_violation};
    use proptest::prelude::*;
    use serde_json::{Value, json};

    fn part(adapter: Adapter, kind: u8, key: u8) -> Value {
        let (name, id) = (format!("f{}", key % 2), format!("c{key}"));
        match (adapter, kind % 4) {
            (_, 0) => json!({"type":"text","text":format!("t{key}")}),
            (_, 1) => json!({"type":"text","text":" "}),
            (Adapter::Anthropic, 2) => json!({"type":"tool_use","id":id,"name":name,"input":{}}),
            (Adapter::Anthropic, _) => {
                json!({"type":"tool_result","tool_use_id":id,"content":"ok"})
            }
            (_, 2) => json!({"functionCall":{"name":name,"id":id,"args":{}}}),
            _ => json!({"functionResponse":{"name":name,"id":id,"response":{}}}),
        }
    }

    fn input(adapter: Adapter, turns: &[(bool, Vec<(u8, u8)>)]) -> Vec<u8> {
        let (field, model) = match adapter {
            Adapter::Anthropic => ("content", "assistant"),
            _ => ("parts", "model"),
        };
        let messages: Vec<_> = turns
            .iter()
            .map(|(user, parts)| {
                let parts: Vec<_> = parts
                    .iter()
                    .map(|(k, key)| part(adapter, *k, *key))
                    .collect();
                json!({"role": if *user { "user" } else { model }, field: parts})
            })
            .collect();
        let mut bytes = vec![u8::from(adapter == Adapter::Gemini)];
        bytes.extend(serde_json::to_vec(&messages).unwrap());
        bytes
    }

    fn batch() -> Vec<Value> {
        let call = |id: &str| json!({"functionCall":{"name":"f","id":id,"args":{}}});
        let response = |id: &str| json!({"functionResponse":{"name":"f","id":id,"response":{}}});
        vec![
            json!({"role":"user","parts":[{"text":"q"}]}),
            json!({"role":"model","parts":[call("a"), call("b")]}),
            json!({"role":"user","parts":[{"text":"interrupt"}, response("b"), response("a")]}),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn structured_turns_are_repaired(
            gemini in any::<bool>(),
            turns in prop::collection::vec(
                (any::<bool>(), prop::collection::vec((0u8..4, 0u8..3), 0..5)),
                0..8,
            ),
        ) {
            let adapter = if gemini { Adapter::Gemini } else { Adapter::Anthropic };
            assert_turns(&input(adapter, &turns));
        }

        #[test]
        fn shuffled_parallel_batches_are_ordered(
            order in Just((0..6u8).collect::<Vec<_>>()).prop_shuffle(),
        ) {
            let calls = (0..6u8).map(|key| (2, key)).collect();
            let responses = order.iter().map(|key| (3, *key)).collect();
            let turns = [(true, vec![(0, 9)]), (false, calls), (true, responses)];
            assert_turns(&input(Adapter::Gemini, &turns));
        }

        #[test]
        fn arbitrary_bytes_keep_the_turn_oracle_total(input in prop::collection::vec(any::<u8>(), 0..512)) {
            assert_turns(&input);
        }
    }

    #[test]
    fn negative_controls_fault_each_production_rule() {
        let model_first = [json!({"role":"model","parts":[{"text":"answer"}]})];
        let assistant_last = [
            json!({"role":"user","content":[{"type":"text","text":"q"}]}),
            json!({"role":"assistant","content":[{"type":"text","text":"a"}]}),
        ];
        for (adapter, messages, violation) in [
            (Adapter::Gemini, &model_first[..], "open"),
            (Adapter::Anthropic, &assistant_last[..], "close"),
        ] {
            let healthy = replay_turns_with(adapter, messages, RULES).unwrap();
            assert_eq!(turn_repair_violation(adapter, messages, &healthy), None);
            let never = Rules {
                boundary: |_| true,
                ..RULES
            };
            let broken = replay_turns_with(adapter, messages, never).unwrap();
            assert_eq!(
                turn_repair_violation(adapter, messages, &broken),
                Some(violation)
            );
        }
        let batch = batch();
        let healthy = replay_turns_with(Adapter::Gemini, &batch, RULES).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Gemini, &batch, &healthy),
            None
        );
        let unordered = Rules {
            order: |_| {},
            ..RULES
        };
        let broken = replay_turns_with(Adapter::Gemini, &batch, unordered).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Gemini, &batch, &broken),
            Some("unordered responses")
        );
        // ANT-TOOL-05: user text between a call and its result merges ahead of
        // the result; production moves results to the front of the turn.
        let text_first = [
            json!({"role":"user","content":[{"type":"text","text":"q"}]}),
            json!({"role":"assistant","content":[{"type":"tool_use","id":"c","name":"f","input":{}}]}),
            json!({"role":"user","content":[{"type":"text","text":"also"}]}),
            json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":"ok"}]}),
        ];
        let healthy = replay_turns_with(Adapter::Anthropic, &text_first, RULES).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Anthropic, &text_first, &healthy),
            None
        );
        assert_eq!(healthy[2]["content"][0]["type"], "tool_result");
        assert_eq!(healthy[2]["content"][1]["text"], "also");
        let broken = replay_turns_with(Adapter::Anthropic, &text_first, unordered).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Anthropic, &text_first, &broken),
            Some("interrupted results")
        );
        // Greptile: responses already in call order but behind text still
        // interrupt the exchange; production moves them to the front.
        let mut interrupted = batch.clone();
        interrupted[2]["parts"].as_array_mut().unwrap().swap(1, 2);
        let healthy = replay_turns_with(Adapter::Gemini, &interrupted, RULES).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Gemini, &interrupted, &healthy),
            None
        );
        assert_eq!(healthy[2]["parts"][2]["text"], "interrupt");
        let broken = replay_turns_with(Adapter::Gemini, &interrupted, unordered).unwrap();
        assert_eq!(
            turn_repair_violation(Adapter::Gemini, &interrupted, &broken),
            Some("unordered responses")
        );
        // Review counterexamples (seed 856): removing an empty turn joins its
        // neighbours, and ordering an unanswered but complete batch moves a
        // result ahead of a blank fragment;
        // without the final normalization a second pass changes either.
        let call = json!({"functionCall":{"name":"f","id":"c","args":{}}});
        let result = |id: &str| json!({"functionResponse":{"name":"f","id":id,"response":{}}});
        let joined = vec![
            json!({"role":"model","parts":[{"type":"text","text":" "}]}),
            json!({"role":"user","parts":[]}),
            json!({"role":"model","parts":[{"type":"text","text":"t0"}]}),
        ];
        let moved = vec![
            json!({"role":"user","parts":[{"text":"q"}]}),
            json!({"role":"model","parts":[call]}),
            json!({"role":"user","parts":[{"type":"text","text":" "}, result("c"), {"type":"text","text":"t0"}, result("d")]}),
        ];
        let fragmented = Rules {
            normalize: |_, _| {},
            ..RULES
        };
        for (messages, text) in [(joined, 1), (moved, 2)] {
            let healthy = replay_turns_with(Adapter::Gemini, &messages, RULES).unwrap();
            assert_eq!(
                turn_repair_violation(Adapter::Gemini, &messages, &healthy),
                None
            );
            let parts = healthy[text]["parts"].as_array().unwrap();
            assert!(parts.iter().any(|part| part["text"] == " t0"));
            let broken = replay_turns_with(Adapter::Gemini, &messages, fragmented).unwrap();
            assert_eq!(
                turn_repair_violation(Adapter::Gemini, &messages, &broken),
                Some("fragmented text")
            );
        }
    }

    #[test]
    fn negative_controls_fault_exact_preservation() {
        let anthropic = [
            json!({"role":"user","content":[{"type":"text","text":"q"}]}),
            json!({"role":"assistant","content":[
                {"type":"tool_use","id":"a","name":"f","input":{}},
                {"type":"tool_use","id":"b","name":"f","input":{}}]}),
            json!({"role":"user","content":[
                {"type":"tool_result","tool_use_id":"a","content":"ok"},
                {"type":"tool_result","tool_use_id":"b","content":"ok"}]}),
        ];
        let gemini = [
            json!({"role":"user","parts":[{"text":"q"}]}),
            json!({"role":"model","parts":[{"text":"answer"}]}),
            json!({"role":"user","parts":[{"text":"more"}]}),
        ];
        let reversed = Rules {
            normalize: |_, parts| parts.reverse(),
            ..RULES
        };
        let replaced = Rules {
            normalize: |_, parts| {
                for part in parts {
                    if let Item::Json(value) = part
                        && value["text"].is_string()
                    {
                        value["text"] = json!("x");
                    }
                }
            },
            ..RULES
        };
        for (adapter, messages, broken, violation) in [
            (
                Adapter::Anthropic,
                &anthropic[..],
                reversed,
                "reordered parts",
            ),
            (Adapter::Gemini, &gemini[..], replaced, "changed text"),
        ] {
            // Valid canonical input passes through repair unchanged...
            assert!(canonical_turns(adapter, messages));
            let healthy = replay_turns_with(adapter, messages, RULES).unwrap();
            assert_eq!(healthy, messages);
            // ...and an idempotent fault that rewrites it is named.
            let faulty = replay_turns_with(adapter, messages, broken).unwrap();
            assert_ne!(faulty, messages);
            assert_eq!(
                turn_repair_violation(adapter, messages, &faulty),
                Some(violation)
            );
        }
    }
    #[test]
    fn negative_control_internal_whitespace_loss_beside_a_blank_fragment() {
        let messages = [json!({"role":"user","content":[
            {"type":"text","text":"a b"},
            {"type":"text","text":" "}]})];
        // Healthy repair joins the blank fragment and keeps every character...
        let healthy = replay_turns_with(Adapter::Anthropic, &messages, RULES).unwrap();
        assert_eq!(healthy[0]["content"][0]["text"], "a b ");
        assert_eq!(
            turn_repair_violation(Adapter::Anthropic, &messages, &healthy),
            None
        );
        // ...while a repair that also squeezes internal whitespace is named,
        // although the blank fragment rules out canonical identity.
        assert!(!canonical_turns(Adapter::Anthropic, &messages));
        let squeezed = Rules {
            normalize: |adapter, parts| {
                (RULES.normalize)(adapter, parts);
                for part in parts {
                    if let Item::Json(value) = part
                        && let Some(text) = value["text"]
                            .as_str()
                            .map(|text| text.split_whitespace().collect::<String>())
                    {
                        value["text"] = json!(text);
                    }
                }
            },
            ..RULES
        };
        let faulty = replay_turns_with(Adapter::Anthropic, &messages, squeezed).unwrap();
        assert_eq!(faulty[0]["content"][0]["text"], "ab");
        assert_eq!(
            turn_repair_violation(Adapter::Anthropic, &messages, &faulty),
            Some("changed text")
        );
    }

    #[test]
    fn checked_in_byte_corpus() {
        for bytes in [
            include_bytes!("../../../fuzz/corpus/turns/anthropic-assistant-first").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/anthropic-empty").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-model-only").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-reversed-batch").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-missing-id").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-call-and-response").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-joined-blank-text").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/anthropic-joined-blank-text").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-result-moved-past-blank").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-ordered-behind-text").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/anthropic-text-before-result").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/gemini-envelope-extra-field").as_slice(),
            include_bytes!("../../../fuzz/corpus/turns/anthropic-spaced-text-beside-blank")
                .as_slice(),
        ] {
            assert_turns(bytes);
        }
    }
}
