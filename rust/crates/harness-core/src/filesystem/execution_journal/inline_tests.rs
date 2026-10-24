use super::*;
use crate::executor::ModelPurpose;
use crate::model::{Model, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest};
use serde_json::Value;

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
            model: Model::new("test".into(), "inline".into(), "1".into(), Value::Null)?,
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
async fn inline_payloads_avoid_generations_and_match_the_file_replay_control() -> Result<()> {
    for native in [false, true] {
        for inline in [false, true] {
            let operation = OperationId::new();
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let (journal, _runtime) = if native {
                owned_journal(operation, Some(&io)).await?
            } else {
                let (journal, issuer) = test_support::journal().await?;
                initialize(&journal, operation).await?;
                (journal, issuer)
            };
            let expected = events();
            for (index, event) in expected.iter().enumerate() {
                append_and_measure(&journal, operation, index, event, inline, &io).await?;
            }
            let records = journal.records(operation, 0, 64, None).await?;
            let mut actual = Vec::new();
            let mut inlined = 0;
            for record in &records {
                if matches!(record.event, ExecutionEvent::ModelInline { .. }) {
                    inlined += 1;
                }
                if let Some(event) =
                    crate::executor::load_model_event(&journal, &record.event, None).await?
                {
                    actual.push(event);
                }
            }
            assert_eq!(actual, expected);
            assert_eq!(inlined, if inline { 5 } else { 0 });
        }
    }
    Ok(())
}

async fn append_and_measure(
    journal: &Journal,
    operation: OperationId,
    index: usize,
    event: &ModelEvent,
    inline: bool,
    original: &PreparationIo,
) -> Result<()> {
    let baseline = original.owned_bytes()?;
    let before = original.receipt();
    let workspace = crate::resources::WorkspaceRef::new(
        journal.volume.provider().clone(),
        journal.volume.storage_name(&mut None)?.into_bytes(),
        None,
    )?;
    let generation_before = journal.host.resolve(&workspace, None).await?.generation;
    let scope = crate::filesystem::PreparationScope::new(Some(original))?;
    let io = scope.io();
    let key = format!("model:0:{index}");
    if inline {
        crate::executor::append_model_event(
            journal,
            operation,
            &key,
            (0, ModelPurpose::Response),
            event,
            io,
        )
        .await?;
    } else {
        // Negative control: the former production path stages a file for
        // every model event before appending its immutable reference.
        let reference = crate::executor::stage_json(journal, operation, &key, event, io).await?;
        journal
            .append(
                operation,
                &key,
                &ExecutionEvent::Model {
                    step: 0,
                    purpose: ModelPurpose::Response,
                    event: reference,
                },
                io,
            )?
            .await?;
    }
    let after = original.receipt();
    let written = after.authority_bytes_written - before.authority_bytes_written
        + after.object_bytes_written
        - before.object_bytes_written;
    let small = crate::contract::canonical_json_bytes(event)?.len() as u64
        <= journal.inline_model_event_bytes();
    let generation_after = journal.host.resolve(&workspace, None).await?.generation;
    assert_eq!(
        generation_before == generation_after,
        inline && small,
        "inline={inline} index={index} written={written}"
    );
    eprintln!("model observation inline={inline} index={index} SDK bytes written={written}");
    drop(scope);
    // The authenticated index legitimately retains funded entries between
    // appends. Release that owner before checking the temporary payload scope.
    journal.verified.lock().await.reset(operation, None)?;
    assert_eq!(original.owned_bytes()?, baseline);
    Ok(())
}

#[tokio::test]
async fn inline_payloads_require_original_read_and_write_grants_and_canonical_bounded_json()
-> Result<()> {
    let (mut journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let valid = String::from_utf8(crate::contract::canonical_json_bytes(&events().remove(0))?)
        .map_err(|_| Error::Invalid("fixture JSON is not UTF-8".into()))?;
    for value in ["{}".into(), format!(" {valid}"), "x".repeat(513)] {
        assert!(
            journal
                .append(
                    operation,
                    "model:0:0",
                    &ExecutionEvent::ModelInline {
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
    let original = journal.scope.clone();
    let issuer = crate::core::AuthorityIssuer::new(
        "append-cost",
        [81; 32],
        journal.verifier.audience().clone(),
    );
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.issue(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            None,
            &mut None,
        )?;
        assert!(
            journal
                .append(
                    operation,
                    "model:0:0",
                    &ExecutionEvent::ModelInline {
                        step: 0,
                        purpose: ModelPurpose::Response,
                        event: valid.clone(),
                    },
                    None
                )?
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
    )
    .await?;
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.issue(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            None,
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

async fn owned_journal(
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<(Journal, crate::core::AuthorityIssuer)> {
    use crate::runtime::DurableTaskHost;
    let fixture = crate::durable_host::tests::fixture::Fixture::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        Some(crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 2,
        }),
        &[],
    )
    .await?;
    let owner = fixture.running(80).await?;
    let (task, fence) = owner.task_binding();
    fixture
        .host
        .claim_model_dispatch(
            task,
            operation,
            0,
            request()?.request_digest(),
            fence.clone(),
        )
        .await?;
    let owner = if io.is_some() {
        fixture.host.journal_owner(task, fence, io).await?
    } else {
        owner
    };
    let journal = fixture.execution(owner, operation)?;
    let files = crate::task_volume::TaskFiles::open(
        fixture.stream.clone(),
        &fixture.storage,
        &fixture.issuer.verifier(&mut None)?,
        &journal.scope,
        65_536,
    )
    .await?;
    let journal = journal.with_task_files(Some(Arc::new(files)));
    initialize(&journal, operation).await?;
    Ok((journal, fixture.issuer))
}

#[tokio::test]
async fn inline_deltas_reach_existing_followers_before_completion_and_retries_heal_publication()
-> Result<()> {
    use crate::task_volume::{Channel, FileKey, Query, TaskFile};
    use futures::{FutureExt, StreamExt};
    for channel in [Channel::Text, Channel::Reasoning] {
        for conditional in [false, true] {
            for omit_publication in [false, true] {
                let operation = OperationId::new();
                let (journal, _runtime) = owned_journal(operation, None).await?;
                let files = journal.task_files.as_ref().unwrap();
                let mut observation = files.inner.observe(Query::Root, None, 16).await?;
                assert!(observation.snapshot.as_ref().unwrap().files.is_empty());
                assert!(observation.changes.next().now_or_never().is_none());
                let event = ExecutionEvent::ModelInline {
                    step: 0,
                    purpose: ModelPurpose::Response,
                    event: String::from_utf8(crate::contract::canonical_json_bytes(
                        &match channel {
                            Channel::Text => ModelEvent::Content {
                                delta: "first delta".into(),
                            },
                            Channel::Reasoning => ModelEvent::Reasoning {
                                delta: "first delta".into(),
                            },
                        },
                    )?)
                    .unwrap(),
                };
                let key = "model:0:0";
                if omit_publication {
                    // Negative control: canonical append without the task-volume projection.
                    assert!(
                        journal
                            .append_validated(operation, Some(2), key, &event, None)
                            .await?
                    );
                } else if conditional {
                    assert!(
                        journal
                            .append_if_tail(operation, 2, key, &event, None)?
                            .await?
                    );
                } else {
                    journal.append(operation, key, &event, None)?.await?;
                }
                let mut chunks = Vec::new();
                while let Some(Some(change)) = observation.changes.next().now_or_never() {
                    let change = change?;
                    for file in change.file.into_iter().chain(change.files) {
                        if matches!(
                            file.key,
                            FileKey::Task {
                                file: TaskFile::Chunk { .. },
                                ..
                            }
                        ) {
                            chunks.push(file);
                        }
                    }
                }
                assert_eq!(chunks.len(), usize::from(!omit_publication));
                if let Some(chunk) = chunks.first() {
                    assert_eq!(chunk.channel, Some(channel));
                    assert_eq!(chunk.bytes.as_ref(), b"first delta");
                }
                assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 3);
                // A matching retry fills an interrupted projection and is idempotent.
                journal.append(operation, key, &event, None)?.await?;
                journal.append(operation, key, &event, None)?.await?;
                let snapshot = files.inner.snapshot(&Query::Root, 16).await?;
                assert_eq!(
                    snapshot
                        .files
                        .iter()
                        .filter(|file| matches!(
                            file.key,
                            FileKey::Task {
                                file: TaskFile::Chunk { .. },
                                ..
                            }
                        ))
                        .count(),
                    1
                );
                assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 3);
            }
        }
    }
    Ok(())
}
