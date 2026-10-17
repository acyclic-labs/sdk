use super::*;
use crate::{
    conversation::{
        ContentPublisher, ConversationMessage, MessageKind, ModelContextSelection,
        ReferencedAttachments,
    },
    core::{Action, Command, SchemaRegistry},
    executor::ExecutionJournal,
    store::StreamAggregate,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn},
};

mod tamper;

#[tokio::test]
async fn task_bound_canonical_private_summary_recovers_without_journal_read_grants() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let agent = fixture
        .signed
        .agent()
        .ok_or_else(|| Error::Invalid("agent".into()))?;
    let private = VolumeRef::new(
        fixture.volume.provider().clone(),
        "summary-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    fixture.filesystem.create_volume(&private).await?;
    fixture.issuer = AuthorityIssuer::new(
        "canonical-owner",
        [103; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "canonical-task".into(),
        },
    );
    let mut grants = fixture
        .signed
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    grants.extend([
        "conversation:bind".into(),
        "conversation:append".into(),
        "conversation:select_context".into(),
        private.capability(VolumeOperation::Read, &mut None)?,
        private.capability(VolumeOperation::Write, &mut None)?,
    ]);
    fixture.signed = fixture.issuer.issue(
        "owner",
        &Capabilities::new(grants.clone()),
        Some(agent),
        None,
        &mut None,
    )?;
    // Storage authority stays host-side; the model cannot read the journal.
    let journal_read = fixture
        .volume
        .capability(VolumeOperation::Read, &mut None)?;
    let journal_write = fixture
        .volume
        .capability(VolumeOperation::Write, &mut None)?;
    grants.retain(|grant| grant != &journal_read && grant != &journal_write);
    fixture.scope = RuntimeScope::new(Capabilities::new(grants), Limits::default())?;
    let reader = crate::filesystem::FilesystemContentVerifier::new(
        fixture.filesystem.clone(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )?;
    let publisher = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &private,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )?;
    let mut aggregate = StreamAggregate::open(
        &fixture.stream,
        (
            fixture.issuer.verifier(&mut None)?.audience().clone(),
            fixture.issuer.verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(reader.clone());
    async fn append(
        aggregate: &mut StreamAggregate<MemoryStream>,
        scope: &Scope,
        action: Action,
    ) -> Result<()> {
        let operation = OperationId::new();
        aggregate
            .execute(
                Command {
                    operation_id: operation,
                    idempotency_key: crate::IdempotencyKey::new(operation.to_string())?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action,
                },
                None,
                &mut None,
            )
            .await?;
        Ok(())
    }
    append(
        &mut aggregate,
        &fixture.signed,
        Action::BindConversation { agent },
    )
    .await?;
    let mut ids = Vec::new();
    for (index, kind) in [MessageKind::User, MessageKind::Assistant, MessageKind::User]
        .into_iter()
        .enumerate()
    {
        let file = publisher
            .stage_once(
                OperationId::new(),
                &format!("history/{index}.txt"),
                b"history",
                "text/plain",
                "history.txt",
                None,
            )?
            .await?
            .0;
        let id = uuid::Uuid::new_v4();
        append(
            &mut aggregate,
            &fixture.signed,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id,
                    sequence: index as u64 + 1,
                    kind,
                    content: file,
                    attachments: ReferencedAttachments::Inline { items: vec![] },
                    reply_to: (index == 1).then(|| ids[0]),
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        )
        .await?;
        ids.push(id);
    }
    let turn = OperationId::from_bytes([106; 16]);
    let selection = ModelContextSelection {
        conversation_revision: 3,
        message_ids: ids,
        checkpoint: None,
    };
    append_selection(&mut aggregate, &fixture.signed, turn, selection.clone()).await?;
    let selected = crate::projection::select_model_context(
        aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("history".into()))?,
        3,
        selection,
        reader.as_ref(),
        Limits::default(),
        Limits::default().attachments,
        None,
    )
    .await?;
    drop(aggregate);
    let (signal, mut accepted) = tokio::sync::mpsc::unbounded_channel();
    let model = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Text("summary".into()),
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
        ])
        .with_dispatch_observer(move |dispatch| {
            signal.send(dispatch).expect("dispatch observer");
        })
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: 1024,
            output_tokens: 64,
        })
        .with_token_counter(|request| {
            Ok(crate::context::ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                10,
                request
                    .request()
                    .messages
                    .iter()
                    .map(|message| {
                        if message
                            .content
                            .file_refs()
                            .any(|file| file.path() == "history/0.txt")
                        {
                            800
                        } else if message
                            .content
                            .file_refs()
                            .any(|file| file.path().starts_with("history/"))
                        {
                            100
                        } else {
                            10
                        }
                    })
                    .collect(),
            ))
        })
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: Some(vec![
                crate::model::ModelEvent::Content {
                    delta: "done".into(),
                },
                crate::model::ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]),
        }]),
    );
    let model_identity = Model::new(
        "scripted".into(),
        "canonical".into(),
        "1".into(),
        Value::Null,
    )?;
    let policy = crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
        response_reserve_tokens: 64,
        recent_tokens: 16,
        retention: Default::default(),
    });
    let runtime = open(&fixture, &private).await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    runtime.task_host().start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let mut execution = runtime
        .stock_execution(
            task,
            fence.clone(),
            turn,
            model_identity.clone(),
            model.clone(),
            ContextPipeline::default(),
            crate::tool::ToolRegistry::default(),
            None,
        )
        .await?
        .with_compaction_policy(policy.clone());
    let input = TurnInput::from_selected_context(execution.operation_id(), selected, 1)?;
    let mut running = Box::pin(execution.execute(input.clone()));
    for _ in 0..2 {
        tokio::select! {
            output = &mut running => panic!("canonical compaction failed before response dispatch: {output:?}"),
            dispatch = accepted.recv() => { dispatch.expect("scripted dispatch"); }
        }
    }
    drop(running);
    assert_eq!(model.requests().len(), 2);
    let checkpoint = crate::executor::canonical_checkpoint_for_operation(
        &execution.journal,
        input.operation_id,
        Limits::default(),
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("canonical checkpoint was not published".into()))?;
    let (envelope, _) = crate::executor::load_canonical_checkpoint(
        &execution.journal,
        &checkpoint,
        Limits::default(),
        None,
        None,
    )
    .await?;
    let proof = crate::executor::load_json::<crate::context::CompactionReference>(
        &execution.journal,
        &envelope.compaction,
        None,
    )
    .await?;
    let summary = proof
        .0
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("summary".into()))?;
    assert_eq!(summary.output.volume(), &private);
    // Negative control: the old checkpoint loader rejects this exact summary.
    assert!(matches!(
        execution.journal.load(&summary.output, None)?.await,
        Err(Error::Unauthorized(_))
    ));
    execution.journal = tamper::check(
        execution.journal,
        input.operation_id,
        &summary.output,
        reader.clone(),
    )
    .await?;
    assert!(
        !runtime
            .task_host()
            .observe_admission(task)
            .await?
            .grants
            .contains(&journal_read)
    );
    drop(execution);
    drop(runtime);
    let reopened = open(&fixture, &private).await?;
    for _ in 0..2 {
        let execution = reopened
            .stock_execution(
                task,
                fence.clone(),
                turn,
                model_identity.clone(),
                model.clone(),
                ContextPipeline::default(),
                crate::tool::ToolRegistry::default(),
                None,
            )
            .await?
            .with_compaction_policy(policy.clone());
        assert_eq!(execution.execute(input.clone()).await?.text, "done");
        assert_eq!(
            model.requests().len(),
            2,
            "recovery dispatched another model effect"
        );
    }
    assert_eq!(model.reconciliations().len(), 1);
    Ok(())
}

async fn open(fixture: &Fixture, private: &VolumeRef) -> Result<Runtime> {
    // This scenario admits two model effects: one summary and one response.
    // The shared deadline/tool fixture admits only a single model effect.
    Runtime::open(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &fixture.volume,
        Some(private),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        &fixture.scope,
        &fixture.tasks,
        &fixture.machines,
        &fixture.tools,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 2,
        },
        1,
        65_536,
        fixture.clock.clone(),
        None,
        &[],
        &ResumableToolRegistry::default(),
        None,
    )
    .await
}

async fn append_selection(
    aggregate: &mut StreamAggregate<MemoryStream>,
    scope: &Scope,
    operation: OperationId,
    selection: ModelContextSelection,
) -> Result<()> {
    aggregate
        .execute(
            Command {
                operation_id: operation,
                idempotency_key: crate::IdempotencyKey::new("canonical-selection")?,
                expected_revision: aggregate.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::SelectModelContext { selection },
            },
            None,
            &mut None,
        )
        .await?;
    Ok(())
}
