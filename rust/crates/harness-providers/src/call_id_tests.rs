use crate::call_ids::CallIds;
use crate::{
    Adapter, projection,
    tests::{provider, request},
};
use acyclic_harness_core::model::{
    ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole, ToolCall,
    ToolResultContent,
};
use proptest::prelude::*;
use serde_json::{Value, json};

fn history(input: &[String], adapter: Adapter) -> (crate::ResolvedModel, ModelRequest) {
    let (provider, _) = provider(adapter, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request
        .tools
        .push(acyclic_harness_core::tool::ToolDefinition {
            name: "echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({}),
            projection_schema: json!({}),
        });
    if input.is_empty() {
        return (provider, request);
    }
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(
            input
                .iter()
                .map(|id| {
                    ModelContentPart::ToolCall(ToolCall::new(id.clone(), "echo".into(), json!({})))
                })
                .collect(),
        ),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Parts(
            input
                .iter()
                .map(|id| ModelContentPart::ToolResult {
                    call_id: id.clone(),
                    name: "echo".into(),
                    content: ToolResultContent::Json { value: json!({}) },
                })
                .collect(),
        ),
    });
    (provider, request)
}

fn projected_ids(body: &Value, adapter: Adapter) -> (Vec<String>, Vec<String>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    fn push(out: &mut Vec<String>, value: &Value) {
        out.push(value.as_str().expect("projected ID").into());
    }
    match adapter {
        Adapter::Anthropic => {
            for message in body["messages"].as_array().unwrap() {
                for part in message["content"].as_array().unwrap() {
                    match part["type"].as_str() {
                        Some("tool_use") => push(&mut calls, &part["id"]),
                        Some("tool_result") => push(&mut results, &part["tool_use_id"]),
                        _ => {}
                    }
                }
            }
        }
        Adapter::Responses => {
            for part in body["input"].as_array().unwrap() {
                match part["type"].as_str() {
                    Some("function_call") => push(&mut calls, &part["call_id"]),
                    Some("function_call_output") => push(&mut results, &part["call_id"]),
                    _ => {}
                }
            }
        }
        Adapter::Chat => {
            for message in body["messages"].as_array().unwrap() {
                if let Some(parts) = message["tool_calls"].as_array() {
                    for part in parts {
                        push(&mut calls, &part["id"]);
                    }
                }
                if message["role"] == "tool" {
                    push(&mut results, &message["tool_call_id"]);
                }
            }
        }
        Adapter::Gemini => {
            for message in body["contents"].as_array().unwrap() {
                for part in message["parts"].as_array().unwrap() {
                    if let Some(call) = part.get("functionCall") {
                        push(&mut calls, &call["id"]);
                    }
                    if let Some(result) = part.get("functionResponse") {
                        push(&mut results, &result["id"]);
                    }
                }
            }
        }
    }
    (calls, results)
}

fn assert_ids(input: Vec<String>) {
    for adapter in [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ] {
        let (provider, request) = history(&input, adapter);
        CallIds::assert_round_trip(&request);
        let table = CallIds::new(&request, adapter);
        let expected: Vec<String> = input
            .iter()
            .map(|id| table.wire(id).unwrap().into_owned())
            .collect();
        let body = projection::body(&provider, &request).unwrap();
        let (calls, results) = projected_ids(&body, adapter);
        assert_eq!(calls, expected);
        assert_eq!(results, expected);
        let encoded = serde_json::to_vec(&request).unwrap();
        let replay = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(body, projection::body(&provider, &replay).unwrap());
        let mut permuted = request.clone();
        permuted.messages.reverse();
        let reordered = CallIds::new(&permuted, adapter);
        for id in &input {
            assert_eq!(table.wire(id).unwrap(), reordered.wire(id).unwrap());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn arbitrary_ids_have_reversible_collision_free_call_and_result_bindings(
        input in prop::collection::vec(prop::collection::vec(any::<char>(), 0..96), 0..12)
    ) { assert_ids(input.into_iter().map(|chars| chars.into_iter().collect()).collect()); }
}

#[test]
fn punctuation_and_reserved_aliases_cannot_collide() {
    assert_ids(vec![
        "call:0".into(),
        "call/0".into(),
        "harness_call_0".into(),
        "call_0".into(),
        "界".repeat(40),
    ]);
    // Negative control: punctuation replacement loses canonical identity.
    let lossy = |id: &str| id.replace([':', '/'], "_");
    assert_eq!(lossy("call:0"), lossy("call/0"));
    let (_, request) = history(&["call:0".into(), "call/0".into()], Adapter::Anthropic);
    let table = CallIds::new(&request, Adapter::Anthropic);
    assert_ne!(table.wire("call:0").unwrap(), table.wire("call/0").unwrap());
}

#[test]
fn checked_in_call_id_seed_exercises_the_shared_fuzzer_oracle() {
    for seed in [
        include_bytes!("../fuzz/corpus/provider_stream/tool-call-ids.json").as_slice(),
        include_bytes!("../fuzz/corpus/provider_stream/alloy-reused-ids.json").as_slice(),
    ] {
        let _: ModelRequest = serde_json::from_slice(&seed[1..]).unwrap();
        crate::fuzzing::assert_stream_invariants(seed);
    }
}

#[test]
fn empty_batch_has_no_calls_or_results() {
    assert_ids(Vec::new());
}

#[test]
fn native_replay_restores_the_original_binding_for_call_and_result() {
    for adapter in [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ] {
        let dispatch = crate::tests::parser_dispatch();
        let call = sampled(adapter, "original_call", dispatch);
        let (_, mut request) = history(std::slice::from_ref(&call.call_id), adapter);
        request.messages[1].content = ModelContent::Part(ModelContentPart::ToolCall(call.clone()));
        let table = CallIds::new(&request, adapter);
        assert_eq!(table.wire(&call.call_id).unwrap(), "original_call");
        CallIds::assert_round_trip(&request);
    }
}

fn sampled(
    adapter: Adapter,
    id: &str,
    dispatch: acyclic_harness_core::model::ModelDispatch,
) -> acyclic_harness_core::model::ToolCall {
    let call = match adapter {
        Adapter::Chat => format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"function":{"name":"echo","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
        ),
        Adapter::Anthropic => format!(
            "data: {}\n\ndata: {}\n\ndata: {}\n\n",
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":id,"name":"echo","input":{}}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_stop"})
        ),
        Adapter::Responses => format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":id,"name":"echo","arguments":"{}"}}),
            json!({"type":"response.completed","response":{"status":"completed"}})
        ),
        Adapter::Gemini => format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":id,"name":"echo","args":{}}}]},"finishReason":"STOP"}]})
        ),
    };
    use futures::TryStreamExt as _;
    let events = futures::executor::block_on(
        crate::stream::events(adapter, crate::tests::chunks(&call, 1), 4096, dispatch)
            .try_collect::<Vec<_>>(),
    )
    .unwrap();
    let encoded = serde_json::to_vec(&events).unwrap();
    let replay: Vec<acyclic_harness_core::model::ModelEvent> =
        serde_json::from_slice(&encoded).unwrap();
    replay
        .into_iter()
        .find_map(|event| match event {
            acyclic_harness_core::model::ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn repeated_provider_ids_across_alloy_turns_keep_distinct_exchanges(
        operation in any::<[u8;16]>(), step in any::<u32>(), first in 0usize..4,
        same_provider in any::<bool>(), same_operation in any::<bool>(),
    ) {
        use acyclic_harness_core::{OperationId, model::{ModelDispatch, PreparedModelRequest}, conversation::Limits};
        let adapters=[Adapter::Anthropic,Adapter::Responses,Adapter::Chat,Adapter::Gemini];
        let first_adapter=adapters[first];
        let next_adapter=adapters[if same_provider {first} else {(first+1)%4}];
        let a=ModelDispatch {operation_id:OperationId::from_bytes(operation),step,request_digest:[0;32]};
        let mut other=operation; other[0]^=1;
        let b=if same_operation {ModelDispatch {step:step.wrapping_add(1),..a}} else {ModelDispatch {operation_id:OperationId::from_bytes(other),..a}};
        let calls=[sampled(first_adapter,"call_0",a),sampled(next_adapter,"call_0",b)];
        prop_assert_ne!(&calls[0].call_id,&calls[1].call_id);
        prop_assert_eq!(&calls[0].call_id,&a.tool_call_id(0));
        prop_assert_eq!(&calls[1].call_id,&b.tool_call_id(0));
        for call in &calls {prop_assert_eq!(&call.metadata["provider_call"]["id"],"call_0");}
        // Negative control: retaining provider strings identifies these as one call.
        prop_assert_eq!(&calls[0].metadata["provider_call"]["id"],&calls[1].metadata["provider_call"]["id"]);
        for target in adapters {
            let (provider, mut request)=history(&[], target);
            for call in &calls {
                request.messages.push(ModelMessage {role:ModelRole::Assistant,content:ModelContent::Part(ModelContentPart::ToolCall(call.clone()))});
                request.messages.push(ModelMessage {role:ModelRole::Tool,content:ModelContent::Part(ModelContentPart::ToolResult {call_id:call.call_id.clone(),name:call.name.clone(),content:ToolResultContent::Json {value:json!({})}})});
            }
            let prepared=PreparedModelRequest::prepare(request,Limits::default(),None).unwrap();
            let restored=PreparedModelRequest::decode(prepared.bytes(),Limits::default(),None).unwrap();
            CallIds::assert_round_trip(restored.request());
            let body=projection::body(&provider,restored.request()).unwrap();
            let (projected_calls,results)=projected_ids(&body,target);
            prop_assert_eq!(projected_calls.len(),2);
            prop_assert_ne!(&projected_calls[0],&projected_calls[1]);
            prop_assert_eq!(&projected_calls,&results);
            let table=CallIds::new(restored.request(),target);
            for (call,wire) in calls.iter().zip(&projected_calls) {
                let mapped=table.wire(&call.call_id).unwrap();
                prop_assert_eq!(mapped.as_ref(),wire);
            }
        }
    }
}
