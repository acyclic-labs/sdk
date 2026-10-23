use crate::{Engine, Script, Step, events_with_usage, last_content};
use proptest::prelude::*;
use serde_json::{Value, json};

proptest! {
    #[test]
    fn gemini_stream_retains_text_calls_and_terminal_usage(text in ".{1,64}", tokens in 0_u64..u64::MAX-1) {
        let mut step: Step = serde_json::from_value(json!({"text":[text,"second"],
            "tools":[{"id":"real-call","name":"read","arguments":{"path":text}}]})).unwrap();
        let frames = |step: &Step| -> Vec<Value> {
            events_with_usage("models/tiny:streamGenerateContent",step,tokens).unwrap()
                .iter().flat_map(|frame| frame.lines())
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str(line).unwrap()).collect()
        };
        let output = frames(&step);
        prop_assert_eq!(output.first().unwrap().pointer("/candidates/0/content/parts/0/text"), Some(&json!(text)));
        prop_assert_eq!(output.get(1).unwrap().pointer("/candidates/0/content/parts/0/text"), Some(&json!("second")));
        prop_assert_eq!(output.get(2).unwrap().pointer("/candidates/0/content/parts/0/functionCall"),
            Some(&json!({"id":"real-call","name":"read","args":{"path":text}})));
        let terminal=output.last().unwrap();
        prop_assert_eq!(terminal.pointer("/candidates/0/finishReason"), Some(&json!("STOP")));
        prop_assert_eq!(&terminal["usageMetadata"], &json!({"promptTokenCount":tokens,"candidatesTokenCount":2,"totalTokenCount":tokens+2}));
        step.hold=true;
        prop_assert!(frames(&step).iter().all(|frame| frame.pointer("/candidates/0/finishReason").is_none()));
        for wrong in ["models/:streamGenerateContent", "models/tiny:generateContent", "models/a/b:streamGenerateContent"] {
            prop_assert!(events_with_usage(wrong,&step,tokens).is_none());
        }
    }
}

#[test]
fn gemini_guards_preserve_latest_user_and_tool_result_without_consuming_mismatches() {
    let script: Script = serde_json::from_value(json!({"cases":{"gemini":{"steps":[
        {"when":{"user":"latest user","tool_result":"real-result"},"text":["done"]}
    ]}}}))
    .unwrap();
    let mut engine = Engine::new(script);
    let body = json!({"contents":[
        {"role":"user","parts":[{"text":"historical user"}]},
        {"role":"model","parts":[{"functionCall":{"id":"call","name":"read","args":{}}}]},
        {"role":"user","parts":[{"functionResponse":{"id":"call","name":"read","response":{"output":"{\"value\":\"real-result\"}"}}},{"text":"latest user"}]}
    ]});
    assert_eq!(last_content(&body, false), "latest user");
    assert_eq!(
        serde_json::from_str::<Value>(&last_content(&body, true)).unwrap(),
        json!({"value":"real-result"})
    );
    let path = "models/tiny:streamGenerateContent";
    assert!(
        engine
            .take(
                "gemini",
                path,
                &json!({"contents":[{"role":"user","parts":[{"text":"wrong"}]}]})
            )
            .is_none()
    );
    assert_eq!(engine.take("gemini", path, &body).unwrap().0, 0);
    assert!(engine.take("gemini", path, &body).is_none());
}
