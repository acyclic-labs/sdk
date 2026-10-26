use crate::{Step, events_with_usage, responses_with_completed_text};
use proptest::prelude::*;
use serde_json::{Value, json};

fn frames(chunks: &[String]) -> Vec<Value> {
    chunks
        .iter()
        .flat_map(|chunk| chunk.lines())
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

proptest! {
    #[test]
    fn authoritative_responses_preserve_distinct_final_text_and_shared_usage(
        streamed in ".{0,64}", final_text in ".{0,64}", tokens in 0_u64..u64::MAX-1,
    ) {
        let mut step = Step { text: vec![streamed.clone()], ..Default::default() };
        let output = frames(&responses_with_completed_text(&step, tokens, &final_text).unwrap());
        let deltas: Vec<_> = output.iter().filter(|frame| frame["type"] == "response.output_text.delta")
            .map(|frame| frame["delta"].as_str().unwrap()).collect();
        prop_assert_eq!(deltas.concat(), streamed.as_str());
        let done = output.iter().find(|frame| frame["type"] == "response.output_item.done").unwrap();
        prop_assert_eq!(done.pointer("/item/content/0/text"), Some(&json!(final_text)));
        let terminal = output.last().unwrap();
        prop_assert_eq!(terminal.pointer("/response/output/0/content/0/text"), Some(&json!(final_text)));
        prop_assert_eq!(terminal.pointer("/response/usage"),
            Some(&json!({"input_tokens":tokens,"output_tokens":2,"total_tokens":tokens+2})));
        prop_assert_eq!(
            responses_with_completed_text(&step, tokens, &streamed),
            events_with_usage("responses", &step, tokens),
        );
        step.hold = true;
        let held = frames(&responses_with_completed_text(&step, tokens, &final_text).unwrap());
        prop_assert!(held.iter().all(|frame| frame["type"] != "response.output_item.done"
            && frame["type"] != "response.completed"));
        prop_assert!(responses_with_completed_text(&step, u64::MAX, &final_text).is_none());
    }
}
