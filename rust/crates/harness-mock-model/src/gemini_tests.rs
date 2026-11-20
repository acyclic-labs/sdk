use crate::{Step, events_with_usage};
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
