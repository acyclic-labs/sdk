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
            compose_tools: false,
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn private_payload_ranges_preserve_original_authority_and_bounded_residency() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let payload = "x".repeat(4 * 1024 * 1024);
    let file = crate::executor::stage_json(&journal, operation, "range", &payload, None).await?;
    let stop = acyclic_fs::CancellationToken::new();
    let range = acyclic_fs::ByteRange {
        offset: 65530,
        length: 16,
    };
    {
        // Original storage chunks are authenticated whole. Credit for exactly
        // one chunk leaves no room for metadata and must refuse before output.
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: 1024 * 1024,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &stop,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let error = journal
            .load_range(&file, range, Some(&io))?
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NativeWorkspace { source, .. }
        if matches!(source.as_ref(), acyclic_fs::WorkspaceError::Work(
            acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }
        ))));
    }
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let bytes = journal.load_range(&file, range, Some(&io))?.await?;
    assert_eq!(bytes.as_ref(), b"xxxxxxxxxxxxxxxx");
    // Residency includes a bounded authenticated chunk and policy metadata.
    // A whole-payload load then slice would exceed this measured bound.
    assert!(work.receipt().peak_allocation_bytes < file.descriptor().byte_length());
    let length = file.descriptor().byte_length();
    assert!(
        journal
            .load_range(
                &file,
                acyclic_fs::ByteRange {
                    offset: length,
                    length: 0
                },
                None
            )?
            .await?
            .is_empty()
    );
    for range in [
        acyclic_fs::ByteRange {
            offset: length,
            length: 1,
        },
        acyclic_fs::ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert!(journal.load_range(&file, range, None)?.await.is_err());
    }
    let foreign = FileRef::new(
        VolumeRef::new(
            journal.volume.provider().clone(),
            "foreign".into(),
            VolumeClass::AgentPrivate,
            crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?,
        file.path().into(),
        file.version().into(),
        file.descriptor().clone(),
        file.display_name().into(),
        &mut None,
    )?;
    assert_eq!(
        journal
            .load_range(&foreign, range, None)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let mut digest = *file.descriptor().sha256();
    digest[0] ^= 1;
    let forged = FileRef::new(
        file.volume().clone(),
        file.path().into(),
        file.version().into(),
        crate::conversation::FileDescriptor::new(
            digest,
            length,
            file.descriptor().media_type().into(),
        )?,
        file.display_name().into(),
        &mut None,
    )?;
    assert!(journal.load_range(&forged, range, None)?.await.is_err());
    assert_eq!(
        journal.load_range(&file, range, None)?.await?.as_ref(),
        bytes.as_ref()
    );
    let reads = io.receipt().backend_read_operations;
    stop.cancel();
    let cancelled = match journal.load_range(&file, range, Some(&io)) {
        Ok(read) => read.await,
        Err(error) => Err(error),
    };
    assert_eq!(
        cancelled.unwrap_err().code(),
        crate::wire::ErrorCode::Cancelled
    );
    assert_eq!(io.receipt().backend_read_operations, reads);
    // Capture the real journal's committed frames, then omit their original
    // atomic proof publications. Reference replay must refuse rather than
    // reconstruct payload bodies to accept an unproven observation.
    use futures::TryStreamExt as _;
    let path = crate::distributed::execution_path(operation, &mut None)?;
    let mut captured = journal
        .stream
        .read(acyclic_stream::ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let source = captured
        .try_next()
        .await?
        .ok_or_else(|| Error::Storage("original journal frame is absent".into()))?;
    let isolated = Arc::new(acyclic_stream::MemoryStream::default());
    let unproven = FilesystemExecutionJournal::new(
        isolated,
        journal.host.clone(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier: &journal.verifier,
            scope: &journal.scope,
            io: None,
        },
    )?;
    unproven
        .path(operation)?
        .append_batch(
            vec![source.value.into()],
            Some(0),
            crate::test_support::fresh_key()?,
        )
        .await?;
    assert!(matches!(unproven.replay(operation, 0, 1, None)?.await,
        Err(Error::Storage(reason)) if reason.contains("original publication proof")));

    // The host's immutable request owner resolves its original publication,
    // rather than reading a whole request or delegating its internal path.
    let write = ContentGrant::verify(
        &journal.verifier,
        &journal.scope,
        &journal.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let read = ContentGrant::verify(
        &journal.verifier,
        &journal.scope,
        &journal.volume,
        VolumeOperation::Read,
        &mut None,
    )?;
    let path = ".system/interactions/approvals/approval-test/request.json";
    let original = journal
        .host
        .put_internal_content(
            &journal.volume,
            &write,
            path,
            payload.as_bytes(),
            "text/plain",
            "request.json",
            None,
            &crate::IdempotencyKey("internal-file:approval-test".into()),
            crate::filesystem::InternalContentClass::Interaction,
            None,
        )
        .await?;
    for offset in [range.offset, payload.len() as u64] {
        let (selected, bytes) = journal
            .host
            .read_internal_range(
                &journal.volume,
                &read,
                path,
                crate::filesystem::InternalContentClass::Interaction,
                acyclic_fs::ByteRange { offset, length: 16 },
                None,
            )
            .await?;
        assert_eq!(selected, original);
        assert_eq!(
            bytes.as_ref(),
            &payload.as_bytes()[offset as usize..][..bytes.len()]
        );
        assert_eq!(
            bytes.len(),
            if offset == payload.len() as u64 {
                0
            } else {
                16
            }
        );
    }
    for (class, offset) in [
        (crate::filesystem::InternalContentClass::Execution, 0),
        (
            crate::filesystem::InternalContentClass::Interaction,
            payload.len() as u64 + 1,
        ),
    ] {
        assert!(
            journal
                .host
                .read_internal_range(
                    &journal.volume,
                    &read,
                    path,
                    class,
                    acyclic_fs::ByteRange { offset, length: 16 },
                    None,
                )
                .await
                .is_err()
        );
    }
    Ok(())
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
    assert_eq!(before.end, 2);
    assert_eq!(before.next, None);
    let quiet =
        crate::executor::usage_page(&journal, operation, "agents/usage", before.end).await?;
    assert!(quiet.items.is_empty());
    assert_eq!(quiet.end, before.end);
    assert_eq!(quiet.next, None);
    let change = async {
        let updates = journal
            .stream
            .follow(journal.follow(operation, quiet.end)?)
            .await?;
        let record = futures::StreamExt::next(&mut acyclic_stream::records_only(updates)).await;
        assert!(matches!(record, Some(Ok(_))));
        Ok::<(), Error>(())
    };
    let mut changed = std::pin::pin!(change);
    assert!(
        futures::poll!(&mut changed).is_pending(),
        "the original producer is quiet"
    );
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
    changed.await?;
    // Registration after the publication still starts at the same exclusive
    // cut, rather than resolving a new head and losing this terminal record.
    let updates = journal
        .stream
        .follow(journal.follow(operation, quiet.end)?)
        .await?;
    assert!(matches!(
        futures::StreamExt::next(&mut acyclic_stream::records_only(updates)).await,
        Some(Ok(_))
    ));
    let records = journal.records(operation, 0, 64, None).await?;
    assert_eq!(records.len(), 3);
    let exported = crate::executor::event_page(&journal, operation, 0).await?;
    assert_eq!(exported.operation, Some(operation));
    assert_eq!(exported.items, records);
    let resumed_events = crate::executor::event_page(&journal, operation, quiet.end).await?;
    assert_eq!(resumed_events.items, records[2..]);
    assert_eq!(resumed_events.end, exported.end);
    assert_ne!(
        exported.items, resumed_events.items,
        "resume replayed the prefix"
    );
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
    assert_eq!(after.end, 3);
    let resumed =
        crate::executor::usage_page(&journal, operation, "agents/usage", quiet.end).await?;
    assert_eq!(resumed.items.len(), 1);
    assert!(resumed.items[0].completed);
    assert_eq!(resumed.items[0].dispatch, before.items[0].dispatch);
    assert_eq!(resumed.end, after.end);
    assert_eq!(resumed.next, None);
    // Rescanning the prior prefix would duplicate the admitted contribution.
    assert!(after.items.len() > resumed.items.len());
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
