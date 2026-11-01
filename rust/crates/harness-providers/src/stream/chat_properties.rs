use super::*;
use acyclic_harness_core::model::ModelDispatch;
use futures::TryStreamExt as _;
use proptest::prelude::*;
use serde_json::json;

fn parse_delta(state: &mut State<'_>, delta: Value) -> Result<Vec<ModelEvent>> {
    // Generated parser inputs, not captured external response fixtures.
    parse(state, &json!({"choices":[{"delta":delta}]}))
}

proptest! {
    #[test]
    fn nullable_optional_deltas_equal_omission_without_retaining_empty_blocks(
        text in ".{0,80}",
    ) {
        let mut omitted = State::new(4096);
        let mut nullable = State::new(4096);
        let expected = parse_delta(&mut omitted, json!({})).unwrap();
        let actual = parse_delta(&mut nullable, json!({
            "content":null, "reasoning_content":null, "reasoning":null, "tool_calls":null
        })).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert!(nullable.thinking.is_empty());
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"reasoning":text})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"reasoning_content":null,"reasoning":text})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"tool_calls":[{"index":0}]})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"tool_calls":[{"index":0,"type":null,"id":null,"function":null}]})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"tool_calls":[{"index":0,"function":{}}]})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"tool_calls":[{"index":0,"function":{"name":null,"arguments":null}}]})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        // Null is absence; a present malformed scalar must still refuse.
        for malformed in [json!(true), json!(7), json!([]), json!({})] {
            prop_assert!(
                parse_delta(&mut State::new(4096), json!({"reasoning_content":malformed})).is_err(),
                "present malformed reasoning must refuse"
            );
            for field in ["content", "tool_calls"] {
                if field == "tool_calls" && malformed.is_array() {
                    continue;
                }
                let mut delta = json!({});
                delta[field] = malformed.clone();
                prop_assert!(parse_delta(&mut State::new(4096), delta).is_err());
            }
        }
    }

    #[test]
    fn candidate_admission_withholds_original_calls_on_malformed_or_late_frames(
        argument in ".{0,80}",
        foreign in 1_u64..u64::MAX,
    ) {
        let arguments = json!({"value":argument}).to_string();
        let initial = json!({"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{
            "index":0,"id":"original-call","type":"function",
            "function":{"name":"original-tool","arguments":arguments}
        }]}}]});
        let complete = json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]});
        let dispatch = ModelDispatch {
            operation_id: acyclic_harness_core::OperationId::from_bytes([1;16]),
            step:0,
            request_digest:[0;32],
        };
        for malformed in [
            json!({"choices":{}}),
            json!({"choices":[null]}),
            json!({"choices":[complete["choices"][0].clone(),{"index":foreign,"delta":{}}]}),
            json!({"choices":[{"index":foreign,"delta":{},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"index":"0","delta":{},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":true,"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":{"role":"user"},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":{"content":[]},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":{"tool_calls":{}},"finish_reason":"tool_calls"}]}),
            json!({"choices":[{"delta":{},"finish_reason":true}]}),
        ] {
            let mut state = State::new(16384);
            prop_assert!(super::super::parse(&mut state, crate::Adapter::Chat, &initial.to_string()).unwrap().is_empty());
            prop_assert!(super::super::parse(&mut state, crate::Adapter::Chat, &malformed.to_string()).is_err());
            prop_assert!(!state.terminal, "malformed candidate committed a terminal state");
            let frames = format!("data: {initial}\n\ndata: {malformed}\n\ndata: [DONE]\n\n");
            let body = futures::stream::iter([Ok(bytes::Bytes::from(frames))]);
            let result = futures::executor::block_on(super::super::events(
                crate::Adapter::Chat, Box::pin(body), 16384, dispatch,
            ).try_collect::<Vec<_>>());
            prop_assert!(result.is_err(), "malformed candidate escaped the original SSE admission");
        }

        let mut state = State::new(16384);
        prop_assert!(parse(&mut state, &initial).unwrap().is_empty());
        // Loss of the original terminal never exposes its accumulated tool call.
        prop_assert!(super::super::finish(state, crate::Adapter::Chat, dispatch).is_err());

        let mut state = State::new(16384);
        prop_assert!(parse(&mut state, &initial).unwrap().is_empty());
        prop_assert!(parse(&mut state, &complete).unwrap().is_empty());
        // Usage-only frames still belong to the original completed response.
        prop_assert!(parse(&mut state, &json!({"choices":[],"usage":{"prompt_tokens":7}})).unwrap().is_empty(), "usage trailer emitted a candidate");
        let events = super::super::finish(state, crate::Adapter::Chat, dispatch).unwrap();
        prop_assert_eq!(events.iter().filter(|event| matches!(event, ModelEvent::ToolCall(_))).count(),1);

        let frames = format!("data: {initial}\n\ndata: {complete}\n\ndata: {initial}\n\ndata: [DONE]\n\n");
        let body = futures::stream::iter([Ok(bytes::Bytes::from(frames))]);
        prop_assert!(futures::executor::block_on(super::super::events(
            crate::Adapter::Chat, Box::pin(body), 16384, dispatch,
        ).try_collect::<Vec<_>>()).is_err(), "late mutation exposed the retained tool batch");

        // The former first-candidate projection discards a real competing candidate
        // and therefore makes the otherwise rejected response executable.
        let competing = json!({"choices":[complete["choices"][0].clone(),{"index":foreign,"delta":{}}]});
        let mut discarded = State::new(16384);
        parse(&mut discarded, &initial).unwrap();
        let first_only = json!({"choices":[competing["choices"].as_array().unwrap().first().unwrap()]});
        parse(&mut discarded, &first_only).unwrap();
        prop_assert!(super::super::finish(discarded, crate::Adapter::Chat, dispatch).unwrap().iter().any(|event| matches!(event, ModelEvent::ToolCall(_))));
    }
}
