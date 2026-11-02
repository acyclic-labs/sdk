//! Generated codec controls; the reference file is real Inspect SDK output,
//! not a provider fixture. Its capture procedure and digest are beside it.
use super::*;
use crate::{
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    model::{Model, ModelDispatch, ModelSampling, ToolCall},
    resources::ProviderRef,
};
use proptest::prelude::*;
use serde_json::{Value, json};

fn admission(request: &[u8]) -> Result<ExecutionRecord> {
    // The export reads no payload through this descriptor; the admission's
    // digest binds the supplied exact reconstructed bytes instead.
    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([5; 16])),
            &mut None,
        )?,
        "request.json".into(),
        "generated-version".into(),
        FileDescriptor::from_bytes(request, "application/json", &mut None)?,
        "request.json".into(),
        &mut None,
    )?;
    Ok(ExecutionRecord {
        operation_id: OperationId::from_bytes([7; 16]),
        sequence: 9,
        idempotency_key: "generated-admission".into(),
        event: ExecutionEvent::ModelStarted {
            step: 2,
            purpose: ModelPurpose::Response,
            request_digest: *blake3::hash(request).as_bytes(),
            request: file,
        },
    })
}

fn source() -> Source<'static> {
    Source {
        scope: "generated-scope",
        agent_path: "agents/generated-agent",
        operation: OperationId::from_bytes([7; 16]),
        source_stream: "generated-journal",
    }
}

fn request(text: String) -> ModelRequest {
    ModelRequest {
        model: Model {
            provider: "generated-binding".into(),
            name: "generated-model".into(),
            revision: "generated-revision".into(),
        },
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                    "generated-call".into(),
                    "read".into(),
                    json!({"path": "example.txt"}),
                ))),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "generated-call".into(),
                    name: "read".into(),
                    content: ToolResultContent::Parts {
                        parts: vec![crate::model::ModelDataPart::Text {
                            text: "Generated tool text".into(),
                        }],
                    },
                }),
            },
        ],
        compose_tools: false,
        tools: Vec::new(),
        sampling: ModelSampling::default(),
        max_output_tokens: None,
    }
}

fn usage(call: &Call<'_>) -> ModelUsageRecord {
    ModelUsageRecord {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch: ModelDispatch {
            operation_id: call.source.operation,
            step: call.step,
            request_digest: call.request_digest,
        },
        purpose: call.purpose,
        provider: call.request.model.provider.clone(),
        model: call.request.model.name.clone(),
        agent_path: Some(call.source.agent_path.into()),
        sampling: Some(call.request.sampling),
        max_output_tokens: None,
        tokens: super::super::super::ModelTokenUsage {
            input: Some(0),
            output: Some(u64::MAX),
            ..Default::default()
        },
        latency_ns: None,
        provider_request_id: None,
        system_fingerprint: None,
        ttft_ns: None,
        cache: None,
    }
}

#[test]
fn codec_preserves_sdk_tool_links_and_original_evidence() -> Result<()> {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/inspect-sdk.json")).map_err(invalid)?;
    let text = reference["samples"][0]["messages"][0]["content"]
        .as_str()
        .expect("captured SDK text");
    let bytes = serde_json::to_vec(&request(text.into())).map_err(invalid)?;
    let admission = admission(&bytes)?;
    let call = Call::new(source(), &admission, &bytes)?;
    let usage = usage(&call);
    let mut output = Vec::new();
    call.write(
        &mut output,
        "2026-11-02T00:00:00Z",
        &ModelContent::Text("Generated final text".into()),
        Some(&usage),
    )?;
    let log: Value = serde_json::from_slice(&output).map_err(invalid)?;
    let sample = &log["samples"][0];
    let messages = sample["messages"].as_array().expect("message sequence");
    let sdk = reference["samples"][0]["messages"]
        .as_array()
        .expect("captured SDK sequence");
    assert_eq!(messages.len(), sdk.len());
    for (message, reference) in messages.iter().zip(sdk) {
        assert_eq!(message["role"], reference["role"]);
    }
    assert_eq!(messages[0]["content"], sdk[0]["content"]);
    assert_eq!(messages[1]["tool_calls"], sdk[1]["tool_calls"]);
    assert_eq!(messages[2]["tool_call_id"], sdk[2]["tool_call_id"]);
    assert_eq!(messages[2]["content"][0]["text"], sdk[2]["content"]);
    assert_eq!(messages[3]["content"], sdk[3]["content"]);
    assert_eq!(
        messages[2]["metadata"]["acyclic_content"],
        serde_json::to_value(&call.request.messages[2].content).map_err(invalid)?
    );
    assert_eq!(sample["id"], sample["metadata"]["replay_key"]);
    assert_eq!(
        sample["id"],
        crate::model::replay_key(&bytes)
            .map(blake3::Hash::from)?
            .to_hex()
            .as_str()
    );
    assert_eq!(
        sample["metadata"]["source"]["operation"],
        json!(admission.operation_id)
    );
    assert_eq!(sample["metadata"]["sequence"], admission.sequence);
    assert_eq!(sample["metadata"]["usage"]["tokens"]["input"], 0);
    assert_eq!(sample["metadata"]["usage"]["tokens"]["output"], u64::MAX);
    assert!(sample["metadata"]["usage"]["tokens"]["cached"].is_null());
    assert_eq!(log["stats"]["started_at"], "");

    let mut foreign = usage.clone();
    foreign.dispatch.step += 1;
    assert!(
        call.write(
            std::io::sink(),
            "2026-11-02T00:00:00Z",
            &ModelContent::Text(String::new()),
            Some(&foreign)
        )
        .is_err()
    );
    let mut foreign = source();
    foreign.operation = OperationId::from_bytes([8; 16]);
    assert!(Call::new(foreign, &admission, &bytes).is_err());
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(Call::new(source(), &admission, &changed).is_err());
    Ok(())
}

proptest! {
    #[test]
    fn arbitrary_text_survives_json_without_coalescing_unknown_measurements(text in any::<String>()) {
        let bytes = serde_json::to_vec(&request(text.clone())).expect("generated request");
        let call = Call::new(source(), &admission(&bytes).expect("descriptor"), &bytes).expect("admission");
        let mut output = Vec::new();
        call.write(&mut output, "2026-11-02T00:00:00Z", &ModelContent::Text(text.clone()), None).expect("encode");
        let log: Value = serde_json::from_slice(&output).expect("decode");
        prop_assert_eq!(&log["samples"][0]["messages"][0]["content"], &json!(text));
        prop_assert_eq!(&log["samples"][0]["messages"][3]["content"], &json!(text));
        prop_assert!(log["samples"][0]["metadata"]["usage"].is_null());
    }
}

#[test]
fn output_failure_stops_without_flush_or_success() -> Result<()> {
    struct Refused {
        remaining: usize,
        writes: usize,
    }
    impl Write for Refused {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            if self.remaining == 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let count = bytes.len().min(self.remaining);
            self.remaining -= count;
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("encoder must not flush")
        }
    }
    let bytes = serde_json::to_vec(&request("generated".into())).map_err(invalid)?;
    let call = Call::new(source(), &admission(&bytes)?, &bytes)?;
    let mut refused = Refused {
        remaining: 0,
        writes: 0,
    };
    assert!(
        call.write(
            &mut refused,
            "2026-11-02T00:00:00Z",
            &ModelContent::Text(String::new()),
            None
        )
        .is_err()
    );
    assert_eq!(refused.writes, 1, "no retry after original output refusal");
    call.write(
        std::io::sink(),
        "2026-11-02T00:00:00Z",
        &ModelContent::Text(String::new()),
        None,
    )?;
    Ok(())
}

#[test]
fn borrowed_encoding_has_no_history_sized_allocations() -> Result<()> {
    for messages in [1, 32, 1024] {
        let mut request = request("generated allocation control".into());
        request.messages = vec![request.messages[0].clone(); messages];
        let bytes = serde_json::to_vec(&request).map_err(invalid)?;
        let call = Call::new(source(), &admission(&bytes)?, &bytes)?;
        let response = ModelContent::Text("generated response".into());
        let borrowed = acyclic_test_support::measure(|| {
            call.write(std::io::sink(), "2026-11-02T00:00:00Z", &response, None)
                .expect("borrowed serialization");
        });
        assert_eq!(borrowed.count_total, 0);
        // A whole-file buffer is the negative control, not an exporter path.
        let buffered = acyclic_test_support::measure(|| {
            let mut output = Vec::new();
            call.write(&mut output, "2026-11-02T00:00:00Z", &response, None)
                .expect("buffered serialization");
            std::hint::black_box(output);
        });
        assert!(buffered.count_total > 0);
    }
    Ok(())
}
