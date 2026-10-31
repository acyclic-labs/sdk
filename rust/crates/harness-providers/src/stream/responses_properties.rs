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
            json!({"type":"custom_tool_call","call_id":"call","name":"tool","input":suffix,"namespace":namespace}),
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

// Generated protocol inputs exercise UTF-8 fragmentation, literal freeform
// input, parallel indices and producer loss; these are not external fixtures.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn freeform_input_survives_fragmentation_and_native_replay(
        input in ".{0,128}", width in 1usize..32,
    ) {
        use acyclic_harness_core::model::{ModelContent, ModelContentPart, ModelMessage, ModelRole, ToolResultContent};
        let (provider, _) = crate::tests::provider(crate::Adapter::Responses, 200);
        let (prepared, dispatch) = crate::tests::request(&provider).unwrap();
        let mut request = prepared.request().clone();
        let frames = [
            json!({"type":"response.output_item.added","output_index":1,"item":{
                "type":"custom_tool_call","call_id":"patch_call","name":"apply_patch","input":""
            }}),
            json!({"type":"response.custom_tool_call_input.delta","output_index":1,"delta":input}),
            json!({"type":"response.custom_tool_call_input.done","output_index":1,"input":input}),
            json!({"type":"response.output_item.done","output_index":0,"item":{
                "type":"function_call","call_id":"shell_call","name":"shell","arguments":"{}"
            }}),
            json!({"type":"response.completed","response":{"status":"completed"}}),
        ];
        let wire: String = frames.iter().map(|frame| format!("data: {frame}\n\n")).collect();
        let events = futures::executor::block_on(super::super::events(
            crate::Adapter::Responses, crate::tests::chunks(&wire, width), 32768, dispatch,
        ).collect::<Vec<_>>());
        let mut calls = Vec::new();
        for event in events {
            match event.unwrap() {
                ModelEvent::ToolCall(call) => calls.push(call),
                ModelEvent::Completed { .. } => {},
                _ => prop_assert!(false, "unexpected event"),
            }
        }
        prop_assert_eq!(calls.len(), 2);
        prop_assert_eq!(&calls[0].call_id, &dispatch.tool_call_id(0));
        prop_assert_eq!(&calls[1].call_id, &dispatch.tool_call_id(1));
        prop_assert_eq!(&calls[1].arguments, &json!({"input":input}));
        let original_call = calls[1].clone();
        for call in &calls {
            request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(call.clone())),
            });
        }
        for call in calls {
            request.messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: call.call_id, name: call.name,
                    content: ToolResultContent::Json { value: json!({"ok":true}) },
                }),
            });
        }
        let prepared = acyclic_harness_core::model::PreparedModelRequest::prepare(
            request, acyclic_harness_core::conversation::Limits::default(), None,
        ).unwrap();
        let restored = acyclic_harness_core::model::PreparedModelRequest::decode(
            prepared.bytes(), acyclic_harness_core::conversation::Limits::default(), None,
        ).unwrap();
        let body = crate::projection::body(&provider, restored.request()).unwrap();
        prop_assert!(crate::validate_input(crate::Adapter::Responses, &body).is_empty());
        let native = body["input"].as_array().unwrap();
        let patch = native.iter().find(|item| item["type"] == "custom_tool_call").unwrap();
        prop_assert_eq!(&patch["input"], &json!(input));
        prop_assert_eq!(&patch["call_id"], &json!("patch_call"));
        prop_assert!(patch.get("arguments").is_none());
        prop_assert!(native.iter().any(|item| item["type"] == "custom_tool_call_output" && item["call_id"] == "patch_call"));
        let mut wrong_kind = body.clone();
        let output = wrong_kind["input"].as_array_mut().unwrap().iter_mut()
            .find(|item| item["type"] == "custom_tool_call_output").unwrap();
        output["type"] = json!("function_call_output");
        prop_assert!(crate::validate_input(crate::Adapter::Responses, &wrong_kind).contains(&"OAR-TOOL-04"));
        for adapter in [crate::Adapter::Chat, crate::Adapter::Anthropic, crate::Adapter::Gemini] {
            prop_assert!(!crate::call_ids::CallIds::new(restored.request(), adapter).unwrap().freeform(&original_call.call_id));
            prop_assert!(crate::projection::body(&crate::tests::provider(adapter, 200).0, restored.request()).is_ok());
        }
        // Dropping the terminal producer ends with the transport's typed
        // interruption, and no tool in the incomplete batch becomes executable.
        let unfinished: String = frames[..4].iter().map(|frame| format!("data: {frame}\n\n")).collect();
        let observed = futures::executor::block_on(super::super::events(
            crate::Adapter::Responses, crate::tests::chunks(&unfinished, width), 32768, dispatch,
        ).collect::<Vec<_>>());
        prop_assert_eq!(observed.len(), 1);
        prop_assert!(matches!(&observed[0], Err(acyclic_harness_core::Error::ModelUnavailable {
            failure: acyclic_harness_core::ModelTransportFailure::Interrupted, ..
        })), "producer loss must return a typed transport interruption");
    }
}

#[test]
fn function_and_freeform_events_cannot_share_an_item() {
    for (kind, events) in [
        (
            "custom_tool_call",
            [
                json!({"type":"response.function_call_arguments.delta","delta":"{}"}),
                json!({"type":"response.function_call_arguments.done","arguments":"{}"}),
            ],
        ),
        (
            "function_call",
            [
                json!({"type":"response.custom_tool_call_input.delta","delta":"patch"}),
                json!({"type":"response.custom_tool_call_input.done","input":"patch"}),
            ],
        ),
    ] {
        for mut event in events {
            let mut state = State::new(16384);
            let added = json!({"type":"response.output_item.added","output_index":0,
                "item":{"type":kind,"name":"tool","call_id":"call"}});
            super::super::parse(&mut state, crate::Adapter::Responses, &added.to_string()).unwrap();
            event["output_index"] = json!(0);
            let used = state.budget.used();
            assert!(
                super::super::parse(&mut state, crate::Adapter::Responses, &event.to_string())
                    .is_err()
            );
            assert_eq!(state.budget.used(), used);
            assert!(state.calls[&0].arguments.is_empty());
        }
    }
}

#[test]
fn responses_call_identity_and_completed_input_are_immutable() {
    for kind in ["function_call", "custom_tool_call"] {
        let original = json!({"type":kind,"id":"item","call_id":"call","name":"tool"});
        for field in ["type", "id", "call_id", "name"] {
            let mut state = State::new(16384);
            let added =
                json!({"type":"response.output_item.added","output_index":0,"item":original});
            super::super::parse(&mut state, crate::Adapter::Responses, &added.to_string()).unwrap();
            let mut changed = original.clone();
            changed[field] = json!(if field == "type" {
                if kind == "function_call" {
                    "custom_tool_call"
                } else {
                    "function_call"
                }
            } else {
                "replacement"
            });
            let done = json!({"type":"response.output_item.done","output_index":0,"item":changed});
            let used = state.budget.used();
            assert!(
                super::super::parse(&mut state, crate::Adapter::Responses, &done.to_string())
                    .is_err()
            );
            assert_eq!(state.budget.used(), used);
            assert_eq!(state.calls[&0].id, "call");
            assert_eq!(state.calls[&0].item_id, "item");
            assert_eq!(state.calls[&0].name, "tool");
        }
        for event_kind in if kind == "function_call" {
            [
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
            ]
        } else {
            [
                "response.custom_tool_call_input.delta",
                "response.custom_tool_call_input.done",
            ]
        } {
            let mut state = State::new(16384);
            super::admit_call(
                &mut state,
                0,
                &original,
                super::Output::admit(&original).unwrap(),
                false,
            )
            .unwrap();
            let event = json!({"type":event_kind,"output_index":0,"item_id":"other",
                "delta":"changed","input":"changed","arguments":"{}"});
            let used = state.budget.used();
            assert!(
                super::super::parse(&mut state, crate::Adapter::Responses, &event.to_string())
                    .is_err()
            );
            assert_eq!(state.budget.used(), used);
            assert!(state.calls[&0].arguments.is_empty());
        }
        let (input_key, delta_kind, done_kind, input) = if kind == "function_call" {
            (
                "arguments",
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
                "{}",
            )
        } else {
            (
                "input",
                "response.custom_tool_call_input.delta",
                "response.custom_tool_call_input.done",
                "日本語\n*** End Patch",
            )
        };
        let mut completed = original.clone();
        completed[input_key] = json!(input);
        let added = json!({"type":"response.output_item.added","output_index":0,"item":original});
        let input_done =
            json!({"type":done_kind,"output_index":0,"item_id":"item",(input_key):input});
        let item_done =
            json!({"type":"response.output_item.done","output_index":0,"item":completed});
        let terminal = json!({"type":"response.completed","response":{"status":"completed","output":[completed]}});
        let mut changed = completed.clone();
        changed[input_key] = json!("replacement");
        for completion in [&input_done, &item_done] {
            for mutation in [
                json!({"type":delta_kind,"output_index":0,"item_id":"item","delta":""}),
                json!({"type":done_kind,"output_index":0,"item_id":"item",(input_key):"replacement"}),
                json!({"type":"response.output_item.added","output_index":0,"item":changed}),
                json!({"type":"response.output_item.done","output_index":0,"item":changed}),
                json!({"type":"response.completed","response":{"status":"completed","output":[changed]}}),
            ] {
                let mut state = State::new(16384);
                for frame in [&added, completion] {
                    super::super::parse(&mut state, crate::Adapter::Responses, &frame.to_string())
                        .unwrap();
                }
                let used = state.budget.used();
                assert!(
                    super::super::parse(
                        &mut state,
                        crate::Adapter::Responses,
                        &mutation.to_string()
                    )
                    .is_err()
                );
                assert_eq!(state.budget.used(), used);
                assert_eq!(state.calls[&0].arguments, input);

                let wire = format!(
                    "data: {added}\n\ndata: {completion}\n\ndata: {mutation}\n\ndata: {terminal}\n\n"
                );
                let events = futures::executor::block_on(
                    super::super::events(
                        crate::Adapter::Responses,
                        crate::tests::chunks(&wire, 1),
                        16384,
                        crate::tests::parser_dispatch(),
                    )
                    .collect::<Vec<_>>(),
                );
                assert_eq!(events.len(), 1);
                assert!(events[0].is_err());
            }
        }
        for delta in [Value::Null, json!(false), json!(0), json!({}), json!([])] {
            let mut state = State::new(16384);
            super::super::parse(&mut state, crate::Adapter::Responses, &added.to_string()).unwrap();
            let used = state.budget.used();
            let event = json!({"type":delta_kind,"output_index":0,"item_id":"item","delta":delta});
            assert!(
                super::super::parse(&mut state, crate::Adapter::Responses, &event.to_string())
                    .is_err()
            );
            assert_eq!(state.budget.used(), used);
            assert!(state.calls[&0].arguments.is_empty());
        }
        // Repeated identical completions preserve the original input and batch.
        let mut state = State::new(16384);
        for frame in [&added, &input_done, &input_done, &item_done, &terminal] {
            super::super::parse(&mut state, crate::Adapter::Responses, &frame.to_string()).unwrap();
        }
        assert_eq!(
            super::super::finish(
                state,
                crate::Adapter::Responses,
                crate::tests::parser_dispatch()
            )
            .unwrap()
            .iter()
            .filter(|event| matches!(event, ModelEvent::ToolCall(_)))
            .count(),
            1
        );
        // Negative control: the same admitted item completes normally, including
        // an empty freeform input and terminal-only delivery of the same call.
        let mut final_item = original.clone();
        final_item[if kind == "function_call" {
            "arguments"
        } else {
            "input"
        }] = json!(if kind == "function_call" { "{}" } else { "" });
        let mut state = State::new(16384);
        let terminal = json!({"type":"response.completed","response":{
            "status":"completed","output":[final_item]
        }});
        super::super::parse(&mut state, crate::Adapter::Responses, &terminal.to_string()).unwrap();
        let events = super::super::finish(
            state,
            crate::Adapter::Responses,
            crate::tests::parser_dispatch(),
        )
        .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelEvent::ToolCall(_)))
                .count(),
            1
        );
    }
}

#[test]
fn malformed_or_omitted_custom_input_cannot_publish_a_partial_batch() {
    let call = json!({"type":"custom_tool_call","id":"item","call_id":"patch",
        "name":"apply_patch","input":""});
    for input in [Value::Null, json!(0), json!({}), json!([]), json!(false)] {
        let mut malformed = call.clone();
        malformed["input"] = input;
        for event in frames(malformed) {
            let mut state = State::new(16384);
            let used = state.budget.used();
            assert!(
                super::super::parse(&mut state, crate::Adapter::Responses, &event.to_string())
                    .is_err()
            );
            assert_eq!(state.budget.used(), used);
            assert!(state.calls.is_empty());
        }
    }
    for output in [json!([]), json!([{"type":"reasoning","id":"replacement"}])] {
        let added = json!({"type":"response.output_item.added","output_index":0,"item":call});
        let terminal =
            json!({"type":"response.completed","response":{"status":"completed","output":output}});
        let wire = format!("data: {added}\n\ndata: {terminal}\n\n");
        let events = futures::executor::block_on(
            super::super::events(
                crate::Adapter::Responses,
                crate::tests::chunks(&wire, 1),
                16384,
                crate::tests::parser_dispatch(),
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(events.len(), 1);
        assert!(events[0].is_err());
    }
    // An explicit empty completed input is valid; an unfinished or omitted
    // input cannot be invented from the initial placeholder at terminal EOF.
    for initial_input in [None, Some(json!("")), Some(json!("unfinished"))] {
        let mut unfinished = call.clone();
        unfinished.as_object_mut().unwrap().remove("input");
        if let Some(input) = initial_input {
            unfinished["input"] = input;
        }
        let added = json!({"type":"response.output_item.added","output_index":0,"item":unfinished});
        let terminal = json!({"type":"response.completed","response":{"status":"completed"}});
        let wire = format!("data: {added}\n\ndata: {terminal}\n\n");
        let events = futures::executor::block_on(
            super::super::events(
                crate::Adapter::Responses,
                crate::tests::chunks(&wire, 1),
                16384,
                crate::tests::parser_dispatch(),
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(events.len(), 1);
        assert!(events[0].is_err());
    }
    let mut omitted = call;
    omitted.as_object_mut().unwrap().remove("input");
    for frame in frames(omitted).into_iter().skip(1) {
        let mut state = State::new(16384);
        assert!(
            super::super::parse(&mut state, crate::Adapter::Responses, &frame.to_string()).is_err()
        );
        assert!(state.calls.is_empty());
    }
}
