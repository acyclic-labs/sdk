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
    assert_eq!(sample["input"], json!([]), "dataset input is unknown");
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
    assert!(matches!(
        call.write(
            std::io::sink(),
            "2026-11-02T00:00:00Z",
            &ModelContent::Text(String::new()),
            Some(&foreign)
        ),
        Err(Error::Invalid(_))
    ));
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
fn admitted_nested_tool_json_survives_the_shared_replay_key() -> Result<()> {
    let mut arguments = json!("original tool argument");
    for _ in 0..127 {
        arguments = Value::Array(vec![arguments]);
    }
    let mut request = request("generated nested request".into());
    let ModelContent::Part(ModelContentPart::ToolCall(tool)) = &mut request.messages[1].content
    else {
        unreachable!("generated tool request")
    };
    tool.arguments = json!({"nested": arguments});
    let bytes = serde_json::to_vec(&request).map_err(invalid)?;
    assert!(
        serde_json::from_slice::<Value>(&bytes).is_err(),
        "former default-depth decoder negative control"
    );
    let call = Call::new(source(), &admission(&bytes)?, &bytes)?;
    assert_eq!(call.request, request);
    let mut output = Vec::new();
    call.write(
        &mut output,
        "2026-11-02T00:00:00Z",
        &ModelContent::Text(String::new()),
        None,
    )?;
    let log: Value = crate::contract::json_from_slice(&output).map_err(invalid)?;
    let ModelContent::Part(ModelContentPart::ToolCall(tool)) = &request.messages[1].content else {
        unreachable!("generated tool request")
    };
    assert_eq!(
        &log["samples"][0]["messages"][1]["tool_calls"][0]["arguments"],
        &tool.arguments
    );
    Ok(())
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
    assert!(matches!(
        call.write(
            &mut refused,
            "2026-11-02T00:00:00Z",
            &ModelContent::Text(String::new()),
            None
        ),
        Err(Error::Storage(_))
    ));
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32"), not(loom)))]
#[tokio::test]
async fn original_terminal_keeps_metadata_and_refuses_foreign_or_unfinished_calls() -> Result<()> {
    use crate::{
        executor::{model_observation, model_usage::ModelCallUsage, stage_json},
        filesystem::MemoryHarnessStorage,
        model::ModelEvent,
    };

    let storage = MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
    let journal = storage.journal();
    let operation = source().operation;
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let request = request("generated committed request".into());
    let bytes = crate::contract::canonical_json_bytes(&request)?;
    let request_file = stage_json(journal.as_ref(), operation, "request", &request, None).await?;
    journal
        .append(
            operation,
            "admission",
            &ExecutionEvent::ModelStarted {
                step: 2,
                purpose: ModelPurpose::Response,
                request_digest: *blake3::hash(&bytes).as_bytes(),
                request: request_file,
            },
            None,
        )?
        .await?;
    let metadata = json!({"usage":{"input_tokens":0,"output_tokens":17},"provider_request_id":"generated-request","finish_reason":"stop"});
    let completed = ModelEvent::Completed {
        metadata: metadata.clone(),
    };
    let usage = ModelCallUsage {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch: ModelDispatch {
            operation_id: operation,
            step: 2,
            request_digest: *blake3::hash(&bytes).as_bytes(),
        },
        purpose: ModelPurpose::Response,
        provider: &request.model.provider,
        model: &request.model.name,
        agent_path: Some("agents/generated-agent"),
        sampling: request.sampling,
        max_output_tokens: None,
        tokens: super::super::super::ModelTokenUsage::reported(&metadata),
        latency_ns: None,
        provider_request_id: Some("generated-request"),
        system_fingerprint: None,
        ttft_ns: None,
        cache: None,
    };
    model_observation::append(
        journal.as_ref(),
        operation,
        "content",
        (2, ModelPurpose::Response),
        &ModelEvent::Content {
            delta: "generated response".into(),
        },
        None,
        None,
    )
    .await?;
    model_observation::append(
        journal.as_ref(),
        operation,
        "terminal",
        (2, ModelPurpose::Response),
        &completed,
        Some(&usage),
        None,
    )
    .await?;
    let records = journal.replay(operation, 0, 8, None)?.await?;
    let call = Call::new(source(), &records[1], &bytes)?;
    assert!(
        call.settlement(journal.as_ref(), &records[2])
            .await
            .is_err()
    );
    let terminal = &records[3];
    let settlement = call.settlement(journal.as_ref(), terminal).await?;
    let mut output = Vec::new();
    settlement.write(
        &mut output,
        "2026-11-02T00:00:00Z",
        &ModelContent::Text("generated response".into()),
    )?;
    let exported: Value = serde_json::from_slice(&output).map_err(invalid)?;
    let exported = &exported["samples"][0]["metadata"];
    assert_eq!(exported["terminal_sequence"], terminal.sequence);
    assert_eq!(exported["terminal_metadata"], metadata);
    assert_eq!(exported["usage"]["tokens"]["input"], 0);
    assert!(exported["usage"]["tokens"]["cached"].is_null());
    let borrowed = acyclic_test_support::measure(|| {
        settlement
            .write(
                std::io::sink(),
                "2026-11-02T00:00:00Z",
                &request.messages[0].content,
            )
            .expect("borrow original terminal metadata");
    });
    assert_eq!(
        borrowed.count_total, 0,
        "original terminal graph is not copied"
    );

    let mut foreign = terminal.clone();
    foreign.operation_id = OperationId::new();
    assert!(call.settlement(journal.as_ref(), &foreign).await.is_err());
    let mut early = terminal.clone();
    early.sequence = records[1].sequence;
    assert!(call.settlement(journal.as_ref(), &early).await.is_err());
    let mut unmeasured = terminal.clone();
    match &mut unmeasured.event {
        ExecutionEvent::Model { usage, .. } | ExecutionEvent::ModelInline { usage, .. } => {
            *usage = None
        }
        _ => unreachable!("original model terminal"),
    }
    let unmeasured = call.settlement(journal.as_ref(), &unmeasured).await?;
    assert!(unmeasured.usage.is_none(), "unknown accounting is not zero");
    let mut mismatched = usage;
    mismatched.tokens.output = Some(18);
    let wrong = stage_json(
        journal.as_ref(),
        operation,
        "wrong-usage",
        &mismatched,
        None,
    )
    .await?;
    let mut changed = terminal.clone();
    match &mut changed.event {
        ExecutionEvent::Model { usage, .. } | ExecutionEvent::ModelInline { usage, .. } => {
            *usage = Some(wrong)
        }
        _ => unreachable!("original model terminal"),
    }
    assert!(call.settlement(journal.as_ref(), &changed).await.is_err());
    Ok(())
}
