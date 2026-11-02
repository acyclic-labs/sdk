use super::*;

pub(super) fn typed_payload_source() -> Context {
    Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("instruction-secret-é🦀".repeat(3_500)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(
                    acyclic_harness::model::ToolCall::new(
                        "previous-call".into(),
                        "echo".into(),
                        serde_json::json!({"value":"é🦀"}),
                    ),
                )),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "previous-call".into(),
                    name: "echo".into(),
                    content: acyclic_harness::model::ToolResultContent::Json {
                        value: serde_json::json!({"value":"é🦀"}),
                    },
                }),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("current".into()),
            },
        ],
        current_input_index: Some(3),
        ..Context::default()
    }
}

#[tokio::test]
pub(super) async fn large_context_payload_preserves_instruction_and_typed_tool_exchange()
-> Result<()> {
    use acyclic_harness::executor::ExecutionEvent;
    let storage = MemoryHarnessStorage::new(AgentId::new(), 262_144).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let limits = Limits {
        file_bytes: 262_144,
        render_bytes: 262_144,
        context_messages: 32,
        attachments: 32,
        path_bytes: 4_096,
        ..Limits::default()
    };
    let source = typed_payload_source();
    let bytes = serde_json::to_vec(&source).map_err(|error| Error::Invalid(error.to_string()))?;
    let projection = journal
        .stage(operation, "typed-payload", &bytes, "application/json", None)?
        .await?;
    assert!(projection.descriptor().byte_length() > MAX_RECORD_BYTES as u64);
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            "prepared",
            &ExecutionEvent::ContextPrepared {
                step: 0,
                projection: projection.clone(),
            },
            None,
        )?
        .await?;
    let records = journal.replay(operation, 1, 1, None)?.await?;
    let Some(ExecutionEvent::ContextPrepared {
        projection: retained,
        ..
    }) = records.first().map(|record| &record.event)
    else {
        return Err(Error::Storage("missing prepared context".into()));
    };
    assert_eq!(retained, &projection);
    let record =
        serde_json::to_vec(&records[0].event).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(record.len() < MAX_RECORD_BYTES);
    assert!(
        !record
            .windows(b"instruction-secret".len())
            .any(|bytes| bytes == b"instruction-secret")
    );
    let bytes = storage.read(retained).await?;
    let reopened: Context =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, source);
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            compose_tools: false,
            model: Model::new("synthetic".into(), "typed-payload".into(), "1".into())?,
            messages: reopened.messages,
            tools: vec![ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: serde_json::json!({"type":"object"}),
                output_schema: serde_json::json!({"type":"object"}),
                projection_schema: acyclic_harness::tool::json_projection_schema(
                    serde_json::json!({"type":"object"}),
                ),
            }],
            sampling: Default::default(),
            max_output_tokens: Some(1_024),
        },
        limits,
        None,
    )?;
    assert_eq!(prepared.request().messages, source.messages);
    Ok(())
}

#[tokio::test]
pub(super) async fn checkpoint_envelope_binds_real_published_context_and_admitted_summary()
-> Result<()> {
    let Fixture {
        storage,
        source,
        compacted,
        reference,
        ..
    } = fixture().await?;
    let operation_id = reference
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing admitted summary".into()))?
        .operation_id;
    let mut payloads = Vec::new();
    for (path, context) in [
        ("checkpoint-source.json", &source),
        ("checkpoint-retained.json", &compacted),
    ] {
        let bytes =
            serde_json::to_vec(context).map_err(|error| Error::Invalid(error.to_string()))?;
        payloads.push(
            storage
                .stage(operation_id, path, &bytes, "application/json", path)
                .await?,
        );
    }
    let proof_bytes = reference.encode()?;
    let proof = storage
        .stage(
            operation_id,
            "checkpoint-proof.json",
            &proof_bytes,
            "application/json",
            "checkpoint-proof.json",
        )
        .await?;
    let checkpoint = acyclic_harness::context::CanonicalContextCheckpoint {
        operation_id,
        selection: acyclic_harness::conversation::ModelContextSelection {
            conversation_revision: 3,
            message_ids: (0..3).map(|_| uuid::Uuid::new_v4()).collect(),
            checkpoint: None,
        },
        source: payloads[0].clone(),
        retained: payloads[1].clone(),
        compaction: proof,
    };
    checkpoint.validate_projection(&source, &compacted, &reference, Limits::default(), None)?;
    assert!(
        checkpoint
            .validate_projection(&source, &source, &reference, Limits::default(), None)
            .is_err()
    );
    let mut changed = checkpoint.clone();
    changed.operation_id = OperationId::new();
    assert!(
        changed
            .validate_projection(&source, &compacted, &reference, Limits::default(), None)
            .is_err()
    );
    let encoded = checkpoint.encode(Limits::default())?;
    let pinned = storage
        .stage(
            operation_id,
            "checkpoint.json",
            &encoded,
            "application/json",
            "checkpoint.json",
        )
        .await?;
    let reader = StoredContent(storage.clone());
    let bytes = reader.read(&pinned, 65_536, None, None)?.await?;
    pinned.descriptor().verify(&bytes, &mut None)?;
    let reopened: acyclic_harness::context::CanonicalContextCheckpoint =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, checkpoint);
    reopened.validate_projection(&source, &compacted, &reference, Limits::default(), None)?;
    Ok(())
}
