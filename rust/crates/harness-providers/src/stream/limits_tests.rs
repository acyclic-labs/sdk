use acyclic_harness_core::{
    Result,
    model::{ModelDispatch, ModelEvent},
};
use serde_json::{Value, json};

use crate::Adapter;

#[test]
fn recorded_parallel_calls_use_byte_capacity_without_a_hidden_item_ceiling() -> Result<()> {
    use futures::TryStreamExt as _;
    // Real llama.cpp b11889/Qwen3-0.6B capture, 2026-10-26; provenance and
    // original requests/headers are beside tool.body. Only call index/ID change.
    let recorded: Vec<_> =
        include_str!("../../../conformance/fixtures/inference/llamacpp/tool.body")
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .collect();
    let deltas: Vec<Value> = recorded
        .iter()
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .filter(|value| value["choices"][0]["delta"]["tool_calls"].is_array())
        .collect();
    assert!(!deltas.is_empty());
    let tail = recorded
        .iter()
        .position(|data| {
            serde_json::from_str::<Value>(data)
                .is_ok_and(|value| value["choices"][0]["finish_reason"] == "tool_calls")
        })
        .expect("original tool completion");
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    for count in [1023, 1024, 1025] {
        let mut frames = Vec::new();
        for index in 0..count {
            for delta in &deltas {
                let mut delta = delta.clone();
                let call = &mut delta["choices"][0]["delta"]["tool_calls"][0];
                call["index"] = json!(index);
                if let Some(id) = call.get_mut("id") {
                    *id = json!(format!(
                        "{}-{index}",
                        id.as_str().expect("recorded call ID")
                    ));
                }
                frames.push(bytes::Bytes::from(format!("data: {delta}\n\n")));
            }
        }
        frames.extend(
            recorded[tail..]
                .iter()
                .map(|data| bytes::Bytes::from(format!("data: {data}\n\n"))),
        );
        for maximum in [2048, 4 * 1024 * 1024] {
            let body = Box::pin(futures::stream::iter(frames.iter().cloned().map(Ok)));
            let output = futures::executor::block_on(
                crate::event_driver::events(Adapter::Chat, body, maximum, dispatch)
                    .try_collect::<Vec<_>>(),
            );
            if maximum == 2048 {
                assert!(output.is_err(), "retained bytes still require capacity");
                continue;
            }
            let output = output?;
            assert_eq!(output.len(), count + 1);
            for (index, event) in output[..count].iter().enumerate() {
                let ModelEvent::ToolCall(call) = event else {
                    panic!("original call missing")
                };
                assert_eq!(call.call_id, dispatch.tool_call_id(index as u64));
                assert_eq!(call.name, "echo");
                assert_eq!(call.arguments, json!({"value":1}));
            }
            let Some(ModelEvent::Completed { metadata }) = output.last() else {
                panic!("original completion missing")
            };
            assert_eq!(metadata["usage"]["prompt_tokens"], 159);
            assert_eq!(metadata["usage"]["completion_tokens"], 19);
        }
    }
    Ok(())
}
