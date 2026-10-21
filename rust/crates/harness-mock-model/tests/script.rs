//! Script codec and consumption properties, with mismatch/terminal controls.
use harness_mock_model::{Engine, Script, Step, bind_result, events, last_content};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #[test]
    fn script_roundtrip_and_namespaces_consume_exactly_once(text in "[^\\p{C}]{0,100}", count in 1usize..12) {
        let steps: Vec<_> = (0..count).map(|_| json!({"text":[text]})).collect();
        let bytes = serde_json::to_vec(&json!({"cases":{"a":{"steps":steps},"b":{"steps":steps}}})).unwrap();
        let script: Script = serde_json::from_slice(&bytes).unwrap();
        let copy: Script = serde_json::from_slice(&serde_json::to_vec(&script).unwrap()).unwrap();
        prop_assert_eq!(&script, &copy);
        let mut engine = Engine::new(script);
        for index in 0..count {
            for namespace in ["a", "b"] {
                let (actual, step) = engine.take(namespace,"responses",&json!({})).unwrap();
                prop_assert_eq!(actual,index);
                for path in ["responses", "chat/completions", "messages"] {
                    let frames = events(path,step).unwrap();
                    for frame in frames {
                        for data in frame.lines().filter_map(|line| line.strip_prefix("data: ")) {
                            if data != "[DONE]" { prop_assert!(serde_json::from_str::<serde_json::Value>(data).is_ok()); }
                        }
                    }
                }
            }
        }
        prop_assert!(engine.take("a","responses",&json!({})).is_none(), "a exhausted");
        prop_assert!(engine.take("b","responses",&json!({})).is_none(), "b exhausted");
        prop_assert!(engine.take("missing","responses",&json!({})).is_none(), "unknown namespace");
    }
}

#[test]
fn guards_use_last_content_and_mismatch_never_consumes() {
    let mut engine = Engine::new(
        serde_json::from_value(json!({"cases":{"case":{"steps":[{
            "when":{"path":"responses","user":"new","tool_result":"result"},"text":["correct"]
        }]}}}))
        .unwrap(),
    );
    let body = json!({"input":[
        {"role":"user","content":[{"text":"old"}]},
        {"role":"user","content":[{"text":"new"}]},
        {"type":"function_call_output","output":"result"}
    ]});
    assert_eq!(last_content(&body, false), "new");
    assert_eq!(last_content(&body, true), "result");
    assert!(engine.take("case", "messages", &body).is_none());
    assert!(
        engine
            .take(
                "case",
                "responses",
                &json!({"input":[{"role":"user","content":"old"}]})
            )
            .is_none()
    );
    assert_eq!(engine.take("case", "responses", &body).unwrap().0, 0);
    assert!(engine.take("case", "responses", &body).is_none());
    let anthropic = json!({"messages":[{"role":"user","content":"new"},{"role":"user","content":[{"type":"tool_result","content":[{"type":"text","text":"result"}]}]}]});
    assert_eq!(last_content(&anthropic, false), "new");
    assert_eq!(last_content(&anthropic, true), "result");
    let chat =
        json!({"messages":[{"role":"user","content":"new"},{"role":"tool","content":"result"}]});
    assert_eq!(last_content(&chat, false), "new");
    assert_eq!(last_content(&chat, true), "result");
}

#[test]
fn counts_do_not_consume_model_steps_but_can_be_scripted_explicitly() {
    let mut engine = Engine::new(serde_json::from_value(json!({"cases":{"case":{"count_bytes":true,"steps":[
        {"text":["answer"]}, {"when":{"path":"count","user":"crash"},"json":{"input_tokens":64},"hold":true}
    ]}}})).unwrap());
    assert_eq!(engine.input_tokens("case", &json!({})), Some(66));
    assert!(engine.take("case", "count", &json!({})).is_none());
    let (index, step) = engine
        .take(
            "case",
            "count",
            &json!({"input":[{"role":"user","content":"crash"}]}),
        )
        .unwrap();
    assert_eq!(index, 1);
    assert!(step.hold);
    assert_eq!(engine.take("case", "responses", &json!({})).unwrap().0, 0);
}

#[test]
fn unknown_fields_fail_and_hold_has_no_terminal_event() {
    assert!(serde_json::from_value::<Script>(json!({"cases":{},"typo":true})).is_err());
    let step: Step = serde_json::from_value(json!({"text":["part"],"hold":true})).unwrap();
    for (path, terminal) in [
        ("responses", "response.completed"),
        ("messages", "message_stop"),
        ("chat/completions", "[DONE]"),
    ] {
        let held = events(path, &step).unwrap().concat();
        assert!(!held.contains(terminal));
        let complete: Step = serde_json::from_value(json!({"text":["part"]})).unwrap();
        assert!(
            events(path, &complete).unwrap().concat().contains(terminal),
            "negative control: completed stream must terminate"
        );
    }
}

proptest! {
    #[test]
    fn bindings_retain_exact_real_result_and_missing_pointer_fails(text in ".{0,128}") {
        let step: Step = serde_json::from_value(json!({"tools":[{"id":"edit","name":"edit","arguments":{"file":{"$tool_result":"/file"}}}]})).unwrap();
        let file = json!({"path":text,"generation":"real-generation"});
        let body = json!({"input":[{"type":"function_call_output","output":json!({"file":file}).to_string()}]});
        let bound = bind_result(&step,&body).unwrap();
        prop_assert_eq!(&bound.tools[0].arguments["file"],&file);
        prop_assert!(bind_result(&step,&json!({"input":[]})).is_err(), "missing real result rejected");
    }
}
