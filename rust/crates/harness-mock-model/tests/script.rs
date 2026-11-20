//! Script codec and consumption properties, with mismatch/terminal controls.
use harness_mock_model::{Engine, Script, Step, bind_result, events, last_content};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #[test]
    fn anthropic_cache_reads_only_the_last_twenty_content_blocks(extra in 0usize..30) {
        let prefix = json!({"type":"text","text":"stable prefix"});
        let request = |extra: usize| {
            let mut blocks = vec![prefix.clone()];
            blocks.extend((0..extra).map(|index| json!({"type":"text","text":index.to_string()})));
            json!({"cache_control":{"type":"ephemeral"},"messages":[{"role":"user","content":blocks}]})
        };
        let mut cache = harness_mock_model::PromptCache::default();
        let first = cache.observe("messages", &request(0), 1);
        prop_assert_eq!(first.read, 0);
        prop_assert!(first.write > 0);
        let next = cache.observe("messages", &request(extra), 1);
        prop_assert_eq!(next.read > 0, extra < 20, "{} added content blocks", extra);
    }
}
fn json_value() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(json!(null)),
        any::<bool>().prop_map(|v| json!(v)),
        any::<i64>().prop_map(|v| json!(v)),
        ".{0,40}".prop_map(|v| json!(v))
    ]
    .prop_recursive(3, 32, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(|v| json!(v)),
            prop::collection::btree_map("[a-z]{0,8}", inner, 0..6).prop_map(|v| json!(v))
        ]
    })
}

proptest! {
    #[test]
    fn json_reply_selection_survives_every_request_value(value in json_value()) {
        let step: Step = serde_json::from_value(json!({"json":{"$request":""}})).unwrap();
        prop_assert_eq!(bind_result(&step,&value).unwrap().json,Some(value));
        let null: Step = serde_json::from_value(json!({"json":null})).unwrap();
        prop_assert_eq!(null.json.as_ref(),Some(&serde_json::Value::Null));
    }

    #[test]
    fn request_templates_preserve_types_and_freeform_bytes(value in json_value(), text in ".{0,128}", line in "[^\\r\\n]{0,128}") {
        let step: Step = serde_json::from_value(json!({
            "text":["echo {{request:/mount~1path}}"],
            "tools":[{"id":"spawn","namespace":"collaboration","name":"spawn_agent","arguments":{"value":{"$request":"/value"}}}],
            "custom_tools":[{"id":"patch","name":"apply_patch","input":"{{request:/mount~1path}}"}],
            "json":{"echo":{"$request":"/value"}, "line":{"$request_line":{"pointer":"/context", "prefix":"Path: ", "suffix":"/file"}}}
        })).unwrap();
        let body = json!({"value":value,"mount/path":text,"context":format!("header\r\nPath: {line}\r\nfooter\n")});
        let bound = bind_result(&step,&body).unwrap();
        prop_assert_eq!(&bound.tools[0].arguments["value"],&value);
        prop_assert_eq!(bound.json.as_ref().unwrap(),&json!({"echo":value,"line":format!("{line}/file")}));
        for context in [json!(line), json!(format!("Path: {line}\nPath: {line}")), value.clone()] {
            let selected: Step = serde_json::from_value(json!({"json":{"$request_line":{"pointer":"", "prefix":"Path: ", "suffix":"/file"}}})).unwrap();
            let actual = bind_result(&selected,&context);
            let expected: Vec<_> = context.as_str().into_iter().flat_map(str::lines).filter_map(|text|text.strip_prefix("Path: ")).collect();
            prop_assert_eq!(actual.is_ok(),expected.len()==1);
            if expected.len()==1 { prop_assert_eq!(actual.unwrap().json,Some(json!(format!("{}/file",expected[0])))); }
        }
        prop_assert_eq!(&bound.text[0],&format!("echo {text}"));
        prop_assert_eq!(&bound.custom_tools[0].input,&text);
        let wire = events("responses",&bound).unwrap().concat();
        let frames: Vec<serde_json::Value> = wire.lines().filter_map(|line|line.strip_prefix("data: ")).map(|line|serde_json::from_str(line).unwrap()).collect();
        let done: Vec<_> = frames.iter().filter(|event|event["type"]=="response.output_item.done" && event.pointer("/item/type").and_then(serde_json::Value::as_str)!=Some("message")).collect();
        prop_assert_eq!(&done[0]["item"]["namespace"],&json!("collaboration"));
        prop_assert_eq!(&done[0]["item"]["arguments"],&json!(bound.tools[0].arguments.to_string()));
        prop_assert_eq!(&done[1]["item"]["type"],&json!("custom_tool_call"));
        prop_assert_eq!(&done[1]["item"]["input"],&json!(text));
        prop_assert!(frames.iter().any(|event|event["type"]=="response.custom_tool_call_input.delta" && event["delta"]==json!(text)));
        for path in ["messages","chat/completions"] { prop_assert!(harness_mock_model::events(path,&bound).is_none()); }
    }
}

#[test]
fn templates_reject_missing_invalid_and_unterminated_pointers_without_reexpanding_data() {
    for value in [
        json!({"$request":"/missing"}),
        json!({"$request":7}),
        json!({"$request_line":{"pointer":"/id", "prefix":"child", "suffix":"", "unknown":true}}),
        json!({"$request_line":{"pointer":"/id", "prefix":7, "suffix":""}}),
        json!("{{request:/missing}}"),
        json!("{{request:/id"),
    ] {
        let step: Step = serde_json::from_value(json!({"json":value})).unwrap();
        assert!(bind_result(&step, &json!({"id":"child"})).is_err());
    }
    let step: Step = serde_json::from_value(json!({"text":["{{request:/id}}"]})).unwrap();
    assert_eq!(
        bind_result(&step, &json!({"id":"{{request:/missing}}"}))
            .unwrap()
            .text,
        ["{{request:/missing}}"]
    );
    for tokens in [u64::MAX - 1, u64::MAX] {
        assert!(
            harness_mock_model::events_with_usage("responses", &Step::default(), tokens).is_none()
        );
    }
}

proptest! {
    #[test]
    fn every_adapter_retains_exact_counted_usage(tokens in 0_u64..u64::MAX-1) {
        let step: Step = serde_json::from_value(json!({"text":["done"]})).unwrap();
        for path in ["responses", "chat/completions", "messages"] {
            let wire = harness_mock_model::events_with_usage(path, &step, tokens).unwrap().concat();
            let events: Vec<serde_json::Value> = wire.lines().filter_map(|line| line.strip_prefix("data: ")).filter_map(|line| serde_json::from_str(line).ok()).collect();
            let usage = events.iter().find_map(|event| event.pointer("/response/usage").or_else(|| event.pointer("/message/usage")).or_else(|| event.get("usage"))).unwrap();
            prop_assert_eq!(usage.get("input_tokens").or_else(|| usage.get("prompt_tokens")).unwrap(), &json!(tokens));
            prop_assert_eq!(&usage["total_tokens"], &json!(tokens+2));
        }
    }

    #[test]
    fn latest_user_and_tool_content_is_independent_across_protocols(text in "[a-zA-Z0-9]{1,40}", files in prop::collection::vec("[a-z]{1,12}",2..6)) {
        let last = files.last().unwrap();
        let step: Step = serde_json::from_value(json!({"tools":[{"id":"edit","name":"edit","arguments":{"file":{"$tool_result":"/file"}}}]})).unwrap();
        let mut parts: Vec<_> = files.iter().map(|file| json!({"type":"tool_result","tool_use_id":file,"content":json!({"file":file}).to_string()})).collect();
        parts.insert(1,json!({"type":"text","text":text}));
        let anthropic = json!({"messages":[{"role":"user","content":"old"},{"role":"user","content":parts}]});
        prop_assert_eq!(last_content(&anthropic,false),text.as_str());
        prop_assert_eq!(serde_json::from_str::<serde_json::Value>(&last_content(&anthropic,true)).unwrap(),json!({"file":last}));
        let bound = bind_result(&step,&anthropic).unwrap();
        prop_assert_eq!(&bound.tools[0].arguments["file"],&json!(last));
        let responses = json!({"input":[{"role":"user","content":[{"text":text}]},{"type":"function_call_output","output":json!({"file":last}).to_string()}]});
        prop_assert_eq!(last_content(&responses,false),text.as_str());
        prop_assert_eq!(serde_json::from_str::<serde_json::Value>(&last_content(&responses,true)).unwrap(),json!({"file":last}));
        let chat = json!({"messages":[{"role":"user","content":text},{"role":"tool","content":json!({"file":last}).to_string()}]});
        prop_assert_eq!(last_content(&chat,false),text.as_str());
        prop_assert_eq!(serde_json::from_str::<serde_json::Value>(&last_content(&chat,true)).unwrap(),json!({"file":last}));
        let gemini = json!({"contents":[
            {"role":"user","parts":[{"text":"historical"}]},
            {"role":"model","parts":[{"functionCall":{"id":"call","name":"read","args":{}}}]},
            {"role":"user","parts":[{"functionResponse":{"id":"call","name":"read","response":{"output":json!({"file":last}).to_string()}}},{"text":text}]}
        ]});
        prop_assert_eq!(last_content(&gemini,false),text.as_str());
        prop_assert_eq!(serde_json::from_str::<serde_json::Value>(&last_content(&gemini,true)).unwrap(),json!({"file":last}));
        let mut engine = Engine::new(serde_json::from_value(json!({"cases":{"case":{"steps":[{"when":{"user":text,"tool_result":last},"text":["matched"]}]}}})).unwrap());
        prop_assert!(engine.take("case","messages",&json!({})).is_none(), "mismatch consumes nothing");
        prop_assert!(engine.take("case","messages",&anthropic).is_some());
        prop_assert!(engine.take("case","messages",&anthropic).is_none(), "matched step consumed once");
    }

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
fn counts_do_not_consume_model_steps_but_can_be_scripted_explicitly() {
    let mut engine = Engine::new(serde_json::from_value(json!({"cases":{"case":{"bytes_per_token":4,"steps":[
        {"text":["answer"]}, {"when":{"path":"count","user":"crash"},"json":{"input_tokens":64},"hold":true}
    ]}}})).unwrap());
    assert_eq!(engine.input_tokens("case", &json!({})), Some(65));
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
fn hold_has_no_terminal_event() {
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
