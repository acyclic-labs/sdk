use super::*;
use crate::executor::ModelPurpose;
use crate::model::{Model, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest};

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn initialize(journal: &Journal, operation: OperationId) -> Result<()> {
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
    let prepared = request()?;
    let reference =
        crate::executor::stage_json(journal, operation, "request", prepared.request(), None)
            .await?;
    journal
        .append(
            operation,
            "model:0:started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: prepared.request_digest(),
                request: reference,
            },
            None,
        )?
        .await
}

fn request() -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("test".into(), "inline".into(), "1".into())?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("input".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )
}

fn events() -> Vec<ModelEvent> {
    vec![
        ModelEvent::Content {
            delta: "hello π\n".into(),
        },
        ModelEvent::Reasoning {
            delta: "thought".into(),
        },
        ModelEvent::ReasoningBlock {
            block: crate::model::ReasoningBlock {
                kind: crate::model::ReasoningKind::Readable,
                text: Some("reason".into()),
                origin: crate::model::ReasoningOrigin {
                    provider: "test".into(),
                    model: "inline".into(),
                    credential_scope: None,
                },
                continuity: Vec::new(),
                outcome: crate::model::ReasoningOutcome::PreservedReadable,
            },
        },
        ModelEvent::ToolCall(crate::model::ToolCall::new(
            "call".into(),
            "example.echo".into(),
            serde_json::json!({"sparse": ["value"]}),
        )),
        ModelEvent::Content {
            delta: "large".repeat(256),
        },
        ModelEvent::Completed {
            metadata: serde_json::json!({"usage": {"input_tokens": 1}}),
        },
    ]
}

#[tokio::test]
async fn terminal_usage_keeps_one_original_commit_and_refuses_foreign_or_partial_records()
-> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let metadata = serde_json::json!({"usage":{"input_tokens":20,"output_tokens":7}});
    let completed = ModelEvent::Completed { metadata };
    let mut usage = crate::executor::model_usage::ModelCallUsage {
        version: 1,
        completed: true,
        dispatch: crate::model::ModelDispatch {
            operation_id: operation,
            step: 0,
            request_digest: request()?.request_digest(),
        },
        purpose: ModelPurpose::Response,
        provider: "test",
        model: "inline",
        agent_path: None,
        tokens: crate::executor::ModelTokenUsage::reported(match &completed {
            ModelEvent::Completed { metadata } => metadata,
            _ => unreachable!(),
        }),
        latency_ns: Some(123),
    };
    let partial = ModelEvent::Content {
        delta: "partial".into(),
    };
    // A staged usage file alone cannot settle or add a journal observation.
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "partial",
            (0, ModelPurpose::Response),
            &partial,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    let before = crate::executor::usage_page(&journal, operation, "agents/usage", 0).await?;
    assert_eq!(
        before.items.len(),
        1,
        "uncommitted usage must stay invisible"
    );
    assert!(!before.items[0].completed);
    assert_eq!(
        before.items[0].tokens,
        crate::executor::ModelTokenUsage::default()
    );
    assert_eq!(before.items[0].latency_ns, None);
    usage.dispatch.operation_id = OperationId::new();
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "foreign",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    usage.dispatch.operation_id = operation;
    usage.tokens.input = Some(21);
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "altered",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    usage.tokens.input = Some(20);
    for _ in 0..2 {
        crate::executor::append_model_event(
            &journal,
            operation,
            "terminal",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None,
        )
        .await?;
    }
    let records = journal.records(operation, 0, 64, None).await?;
    assert_eq!(records.len(), 3);
    let ExecutionEvent::Model {
        usage: Some(reference),
        ..
    } = &records[2].event
    else {
        panic!("original terminal commit lost its usage reference");
    };
    let bytes = journal.load(reference, None)?.await?;
    let record: crate::executor::ModelUsageRecord = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(record.dispatch.operation_id, operation);
    assert_eq!(record.tokens, usage.tokens);
    assert_eq!(record.latency_ns, Some(123));
    assert_eq!(record.agent_path, None);
    let after = crate::executor::usage_page(&journal, operation, "agents/usage", 0).await?;
    assert_eq!(after.items.len(), 2);
    assert!(!after.items[0].completed);
    assert!(after.items[1].completed);
    assert_eq!(after.items[0].dispatch, after.items[1].dispatch);
    assert_eq!(after.items[1].tokens, usage.tokens);
    Ok(())
}

#[tokio::test]
async fn private_payloads_reject_inline_bodies_at_every_append_entry_point() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let valid = String::from_utf8(crate::contract::canonical_json_bytes(&events().remove(0))?)
        .map_err(|_| Error::Invalid("fixture JSON is not UTF-8".into()))?;
    assert_eq!(journal.inline_model_event_bytes(), 0);
    // Negative control: even valid, fully authorized small model content must
    // never be accepted as raw Stream data by either append entry point.
    let inline = ExecutionEvent::ModelInline {
        usage: None,
        step: 0,
        purpose: ModelPurpose::Response,
        event: valid.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 2, "model:0:0", &inline, None)?
            .await
            .is_err()
    );
    for value in [
        valid.clone(),
        "{}".into(),
        format!(" {valid}"),
        "x".repeat(513),
    ] {
        assert!(
            journal
                .append(
                    operation,
                    "model:0:0",
                    &ExecutionEvent::ModelInline {
                        usage: None,
                        step: 0,
                        purpose: ModelPurpose::Response,
                        event: value,
                    },
                    None
                )?
                .await
                .is_err()
        );
    }
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn private_payloads_require_original_read_and_write_grants() -> Result<()> {
    let (mut journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let original = journal.scope.clone();
    let issuer = crate::core::AuthorityRoot::new(
        "append-cost",
        [81; 32],
        journal.verifier.audience().clone(),
    );
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.bootstrap_scope(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        assert!(
            crate::executor::append_model_event(
                &journal,
                operation,
                "model:0:0",
                (0, ModelPurpose::Response),
                &events().remove(0),
                None,
                None
            )
            .await
            .is_err(),
            "missing opposite grant for {grant:?}"
        );
    }
    journal.scope = original;
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    crate::executor::append_model_event(
        &journal,
        operation,
        "model:0:0",
        (0, ModelPurpose::Response),
        &events().remove(0),
        None,
        None,
    )
    .await?;
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.bootstrap_scope(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        assert_eq!(
            journal.records(operation, 0, 64, None).await.is_ok(),
            grant == VolumeOperation::Read,
            "cached replay requires read authority"
        );
    }
    Ok(())
}
