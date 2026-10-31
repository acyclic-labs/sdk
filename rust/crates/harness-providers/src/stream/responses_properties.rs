use super::*;
use futures::StreamExt as _;
use proptest::prelude::*;
use serde_json::json;

fn frames(item: Value) -> [Value; 4] {
    [
        json!({"type":"response.output_item.added","output_index":0,"item":item}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"output":[item]}}),
        json!({"type":"response.incomplete","response":{
            "output":[item],"incomplete_details":{"reason":"max_output_tokens"}
        }}),
    ]
}

// Generated admission mutations, not captured external transcripts.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn unsupported_output_cannot_become_a_successful_partial_decode(
        suffix in ".{0,40}", namespace in ".{0,40}", chunk in 1usize..32,
    ) {
        let dispatch = acyclic_harness_core::model::ModelDispatch {
            operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
            step: 0,
            request_digest: [0; 32],
        };
        let ordinary = json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"function_call","call_id":"call","name":"tool","arguments":"{}"
        }});
        let terminal = json!({"type":"response.completed","response":{"status":"completed"}});
        for item in [
            json!({"type":format!("unsupported_{suffix}")}),
            json!({"type":"custom_tool_call","call_id":"call","name":"tool","input":suffix}),
            json!({"type":"web_search_call","id":"search","status":"completed"}),
            json!({"type":"function_call","call_id":"call","name":"tool","arguments":"{}","namespace":namespace}),
            json!({"type":null}),
            json!(false),
        ] {
            for frame in frames(item) {
                let mut state = State::new(16384);
                let used = state.budget.used();
                let result = super::super::parse(&mut state, crate::Adapter::Responses, &frame.to_string());
                prop_assert!(result.is_err(), "unsupported semantics must refuse at every output boundary");
                prop_assert!(!state.terminal, "unsupported output must not commit completion");
                prop_assert!(state.calls.is_empty() && state.thinking.is_empty());
                prop_assert_eq!(state.budget.used(), used);

                // A later unsupported item must also stop the original stream
                // before an earlier complete call can reach the executor.
                let input = format!("data: {ordinary}\n\ndata: {frame}\n\ndata: {terminal}\n\n");
                let observed = futures::executor::block_on(super::super::events(
                    crate::Adapter::Responses, crate::tests::chunks(&input, chunk),
                    16384, dispatch,
                ).collect::<Vec<_>>());
                prop_assert_eq!(observed.iter().filter(|event| event.is_err()).count(), 1);
                prop_assert!(!observed.iter().any(|event| matches!(event,
                    Ok(ModelEvent::ToolCall(_) | ModelEvent::Completed { .. })
                )), "partial batches must not become executable or complete");
            }
        }

        // Null uses the shared omission rule; ordinary calls remain executable.
        for namespace in [None, Some(Value::Null)] {
            let mut item = json!({"type":"function_call","call_id":"call","name":"tool","arguments":"{}"});
            if let Some(namespace) = namespace {
                item["namespace"] = namespace;
            }
            let mut state = State::new(16384);
            for frame in frames(item).into_iter().take(3) {
                super::super::parse(&mut state, crate::Adapter::Responses, &frame.to_string()).unwrap();
            }
            let events = super::super::finish(state, crate::Adapter::Responses, dispatch).unwrap();
            prop_assert!(events.iter().any(|event| matches!(event, ModelEvent::ToolCall(call) if call.name == "tool" && call.arguments == json!({}))), "ordinary unnamespaced calls must survive admission");
        }

        let input = format!("data: {ordinary}\n\ndata: {terminal}\n\n");
        let observed = futures::executor::block_on(super::super::events(
            crate::Adapter::Responses, crate::tests::chunks(&input, chunk),
            16384, dispatch,
        ).collect::<Vec<_>>());
        prop_assert!(observed.iter().all(|event| event.is_ok()));
        prop_assert!(observed.iter().any(|event| matches!(event, Ok(ModelEvent::ToolCall(_)))));
        prop_assert!(observed.iter().any(|event| matches!(event, Ok(ModelEvent::Completed { .. }))), "ordinary streams must still terminate successfully");
    }
}
