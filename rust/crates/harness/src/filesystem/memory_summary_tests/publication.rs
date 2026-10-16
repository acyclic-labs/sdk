use super::*;
use crate::tool::ToolRegistry;

#[allow(
    clippy::too_many_lines,
    reason = "one actual parent compaction, child capture, retry and publication scenario"
)]
async fn summary_parent_inputs(
    inputs: [&[u8]; 2],
    model: Arc<ScriptedModelProvider>,
) -> Result<(
    MemoryHarnessStorage,
    crate::bundle::HarnessBundle,
    FileRef,
    Limits,
)> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 262_144).await?;
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let bundle = storage
        .builder()
        .model(
            Model::new(
                "synthetic".into(),
                "summary-fork".into(),
                "1".into(),
                Value::Null,
            )?,
            model.clone(),
        )
        .grant("model:generate")
        .limits(limits)
        .build()?;
    let mut last = OperationId::new();
    for index in 0..2 {
        last = OperationId::new();
        let input = storage
            .stage(
                last,
                &format!("turns/{index}/input.txt"),
                inputs[index],
                "text/plain",
                "input.txt",
            )
            .await?;
        storage
            .run_conversation(&bundle, last, input, Vec::new(), 1)
            .await?;
    }
    assert_eq!(model.dispatches().len(), 3);
    {
        let aggregate = storage.journal.conversation_projection(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), 4);
        assert_eq!(conversation.resident_after_sequence(), 3);
        assert_eq!(conversation.messages().len(), 1);
        assert_eq!(conversation.messages()[0].sequence, 4);
    }
    let reference = crate::executor::canonical_checkpoint_for_operation(
        storage.journal.as_ref(),
        last,
        limits,
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("missing admitted checkpoint".into()))?;
    Ok((storage, bundle, reference, limits))
}

async fn summary_preparation(
    inputs: [&[u8]; 3],
    model: Arc<ScriptedModelProvider>,
) -> Result<SummaryFixture> {
    let (storage, bundle, reference, limits) =
        summary_parent_inputs([inputs[0], inputs[1]], model.clone()).await?;
    let later = OperationId::new();
    let later_input = storage
        .stage(
            later,
            "turns/later/input.txt",
            inputs[2],
            "text/plain",
            "later-input.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, later, later_input, Vec::new(), 1)
        .await?;
    assert_eq!(model.remaining(), 0);
    let later_checkpoint = crate::executor::canonical_checkpoint_for_operation(
        storage.journal.as_ref(),
        later,
        limits,
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("missing later admitted checkpoint".into()))?;
    assert_ne!(later_checkpoint, reference);
    let mut aggregate = StreamAggregate::open(
        &storage.stream,
        (
            storage.conversation.clone(),
            storage.verifier(),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    let conversation = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: conversation.logical_revision(),
        message_ids: conversation
            .messages()
            .iter()
            .map(|message| message.id)
            .collect(),
        checkpoint: Some(later_checkpoint),
    };
    aggregate
        .execute(
            Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("summary-parent-complete-cut")?,
                expected_revision: aggregate.reducer().revision(),
                scope: storage.scope.clone(),
                causal_parent: None,
                action: Action::SelectModelContext { selection },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    aggregate.compact_conversation_projection()?;
    let retired = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
    assert_eq!(retired.logical_revision(), 6);
    assert_eq!(retired.resident_after_sequence(), 6);
    assert!(retired.messages().is_empty());

    summary_fork_fixture(storage, aggregate, reference, limits).await
}

async fn summary_fork_fixture(
    storage: MemoryHarnessStorage,
    aggregate: StreamAggregate<MemoryStream>,
    reference: FileRef,
    limits: Limits,
) -> Result<SummaryFixture> {
    let agent = storage
        .scope
        .agent()
        .ok_or_else(|| Error::Invalid("missing agent".into()))?;
    let through = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("missing conversation".into()))?
        .logical_revision();
    let history = HistoryReader::new(
        &storage.stream,
        &storage.conversation,
        &(storage.verifier()),
        &mut None,
    )?;
    let cursor = history.pin(0, None).await?;
    let original_scope = RuntimeScope::new(storage.scope.capabilities().clone(), limits)?;
    let prepared = prepare_summary_fork_context(
        aggregate.reducer(),
        SummaryForkSelection {
            checkpoint: reference,
            limits,
            history_limits: HistoryReadLimits {
                maximum_events: 16,
                maximum_bytes: 2_097_152,
            },
        },
        storage.journal.as_ref(),
        &original_scope,
        &history,
        &cursor,
        storage.content_verifier.as_ref(),
    )
    .await?;
    let proof: crate::context::CompactionReference = crate::contract::json_from_slice(
        &storage
            .journal
            .load(&prepared.checkpoint().compaction, None)?
            .await?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(proof.summary.is_some());
    let source = prepared.context().clone();
    let selection = prepared.selection().clone();
    let provider = storage.volume.provider().clone();
    let project = VolumeRef::new(
        provider.clone(),
        "summary-parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("summary-project".into()),
        &mut None,
    )?;
    let project_head = storage.host.create_volume(&project).await?;
    let grants = Capabilities::new(
        storage
            .scope
            .capabilities()
            .iter()
            .map(str::to_owned)
            .chain([
                "fork:publish".to_owned(),
                project.capability(VolumeOperation::Read, &mut None)?,
            ]),
    );
    let scope = storage
        .issuer
        .issue("summary-parent", &grants, Some(agent), None, &mut None)
        .expect("scope proof");
    let stream_provider = ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None)?;
    let archived = history
        .fork_history(
            aggregate.reducer(),
            through,
            HistoryReadLimits {
                maximum_events: u32::try_from(through)
                    .map_err(|e| Error::Invalid(e.to_string()))?,
                maximum_bytes: 2_097_152,
            },
            &mut None,
        )
        .await?;
    let preparer = FilesystemForkPreparer::new(
        storage.host.clone(),
        aggregate.reducer().copy(&mut None)?,
        storage.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        storage.content_verifier.clone(),
        Some(archived),
        None,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?
    .with_summary_context(
        prepared,
        storage.journal(),
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?;
    let child_agent = AgentId::new();
    let child_private = VolumeRef::new(
        provider.clone(),
        "summary-child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let request = ForkRequest {
        authorized_source: None,
        operation_id: OperationId::new(),
        parent: storage.conversation.clone(),
        parent_revision: aggregate.reducer().revision(),
        child: Authority {
            kind: AggregateKind::Conversation,
            id: "summary-child".into(),
        },
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_result_volume: None,
            summary: None,
            child_project_volume: VolumeRef::new(
                provider.clone(),
                "summary-child-project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("summary-project".into()),
                &mut None,
            )?,
            child_private_volume: child_private.clone(),
            inherited_through_sequence: 0,
            maximum_inherited_messages: 16,
            maximum_inherited_bytes: 262_144,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection {
                required: true,
                revision: ResourceRevision::PrivateVolume {
                    volume: storage.volume.clone(),
                    generation: storage
                        .host
                        .resolve(
                            &crate::filesystem::workspace_ref(
                                storage.volume.provider().clone(),
                                &storage.volume.storage_name(&mut None)?,
                            )?,
                            None,
                        )
                        .await?
                        .generation,
                    paths: Vec::new(),
                },
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(),
                    storage.conversation.stream_path(&mut None)?.into_bytes(),
                    Some(aggregate.reducer().revision().to_string()),
                )?),
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::Project {
                    volume: project,
                    generation: project_head.generation,
                },
            },
        ],
        boundary: None,
    }
    .with_history_policy(
        aggregate.reducer(),
        ForkHistoryPolicy::Summary(Box::new(selection)),
        &mut None,
    )?;
    Ok(SummaryFixture {
        storage,
        aggregate,
        preparer,
        request,
        source,
        scope,
        stream_provider,
        child_private,
        provider,
        limits,
    })
}

struct SummaryFixture {
    storage: MemoryHarnessStorage,
    aggregate: StreamAggregate<MemoryStream>,
    preparer: FilesystemForkPreparer<MemoryAuthorityBackend, MemoryObjectBackend>,
    request: ForkRequest,
    source: Context,
    scope: Scope,
    stream_provider: ProviderRef,
    child_private: VolumeRef,
    provider: ProviderRef,
    limits: Limits,
}

async fn published_summary(
    inputs: [&[u8]; 3],
    model: Arc<ScriptedModelProvider>,
) -> Result<(
    MemoryHarnessStorage,
    StreamAggregate<MemoryStream>,
    crate::fork::ForkSeed,
    Context,
    Limits,
)> {
    let SummaryFixture {
        storage,
        mut aggregate,
        preparer,
        request,
        source,
        scope,
        stream_provider,
        child_private,
        provider,
        limits,
    } = summary_preparation(inputs, model).await?;
    let (quota_preparation, _quota_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1, 134_217_728).await?;
    assert!(
        preparer
            .prepare_for_task(&request, &Arc::new(quota_preparation), None)
            .await
            .is_err()
    );
    let child_private_workspace =
        workspace_ref(provider.clone(), &child_private.storage_name(&mut None)?)?;
    assert!(matches!(
        storage.host.resolve(&child_private_workspace, None).await,
        Err(Error::NotFound(_))
    ));
    let (cancelled_preparation, cancelled_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1_000_000, 134_217_728).await?;
    cancelled_invocation.0.cancel();
    assert!(
        preparer
            .prepare_for_task(&request, &Arc::new(cancelled_preparation), None)
            .await
            .is_err()
    );
    assert!(matches!(
        storage.host.resolve(&child_private_workspace, None).await,
        Err(Error::NotFound(_))
    ));
    let (task_preparation, _successful_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1_000_000, 134_217_728).await?;
    let task_preparation = Arc::new(task_preparation);
    let lost_acknowledgement = preparer
        .prepare_for_task(&request, &task_preparation, None)
        .await?;
    let preparer = preparer.with_task_preparation(task_preparation)?;
    let report = aggregate.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(report, lost_acknowledgement);
    assert_eq!(
        aggregate.prepare_fork(&preparer, request.clone()).await?,
        report
    );
    assert_eq!(
        aggregate.reconcile_fork(&preparer, request.clone()).await?,
        Some(report.clone())
    );
    let capture = report
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing Summary capture".into()))?;
    assert!(!capture.payloads.is_empty());
    assert_eq!(report.inherited_through_sequence, 6);
    let bytes = storage
        .host
        .read_pinned_io(&capture.context, 262_144, None)
        .await?;
    let captured: Context = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(captured.messages.len(), source.messages.len());
    assert_eq!(captured.current_input_index, None);
    let originals = source
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(source.metadata.values())
        .map(|file| {
            Ok((
                hex::encode(crate::executor::json::canonical_json_digest(
                    file,
                    u64::MAX,
                    &mut None,
                )?),
                file.clone(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for copy in &capture.payloads {
        let key = copy
            .path()
            .rsplit('/')
            .next()
            .ok_or_else(|| Error::Invalid("missing copy key".into()))?;
        let original = originals
            .get(key)
            .ok_or_else(|| Error::Invalid("copy is not source-bound".into()))?;
        assert_eq!(copy.volume(), &child_private);
        assert_eq!(copy.descriptor(), original.descriptor());
        assert_eq!(
            storage
                .host
                .read_pinned_io(copy, 262_144, None)
                .await?
                .as_ref(),
            storage.journal.load(original, None)?.await?.as_ref()
        );
    }
    let seed = report.into_seed(&mut None)?;
    assert!(seed.history_witness.is_some());
    for mutation in 0..4 {
        let mut changed = seed.clone();
        match mutation {
            0 => {
                changed.history_witness = None;
            }
            1 => {
                changed.reference_grants.pop();
            }
            2 => {
                changed.attached_agents.push(AgentId::new());
            }
            _ => {
                changed.inherited_through_sequence -= 1;
            }
        }
        let mut reducer = aggregate.reducer().copy(&mut None)?;
        assert!(
            reducer
                .apply(&Command {
                    operation_id: changed.operation_id,
                    idempotency_key: IdempotencyKey::new(format!(
                        "summary-witness-mutation-{mutation}"
                    ))?,
                    expected_revision: reducer.revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::PublishFork {
                        seed: Box::new(changed)
                    },
                })
                .is_err()
        );
    }
    let fs_verifier = FilesystemForkVerifier::new(storage.host.clone(), 262_144, None, None)?;
    fs_verifier.verify(&seed, &mut None)?.await?;
    assert!(matches!(
        storage
            .host
            .claim_fork_seed(&seed, aggregate.reducer(), &storage.verifier(), &scope)
            .await,
        Err(Error::Unsupported(_))
    ));
    let mut changed = seed.clone();
    changed
        .summary
        .as_mut()
        .ok_or_else(|| Error::Invalid("missing capture".into()))?
        .selection
        .history_limits
        .maximum_events += 1;
    assert!(matches!(
        fs_verifier.verify(&changed, &mut None)?.await,
        Err(Error::Conflict(_))
    ));
    aggregate = publish_summary_seed(&storage, aggregate, &seed, scope, &stream_provider).await?;
    assert_eq!(aggregate.reducer().fork(&seed.child), Some(&seed));
    Ok((storage, aggregate, seed, captured, limits))
}

#[allow(
    clippy::too_many_lines,
    reason = "one child binding/import/admission with denied scope and narrowed bounds"
)]
async fn assert_summary_child_import(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    seed: &crate::fork::ForkSeed,
    captured: &Context,
    limits: Limits,
) -> Result<()> {
    use crate::context::{ContextPipeline, ContextStage};
    use crate::executor::{Executor, StockExecutor};
    let issuer = AuthorityIssuer::new("summary-child", [42; 32], seed.child.clone());
    let read_grants = seed.reference_capabilities(seed.child_agent)?;
    let grants = Capabilities::new(
        read_grants.iter().map(str::to_owned).chain([
            "conversation:bind".to_owned(),
            "model:generate".to_owned(),
            seed.child_private_volume
                .capability(VolumeOperation::Write, &mut None)?,
            seed.child_private_volume
                .capability(VolumeOperation::Read, &mut None)?,
        ]),
    );
    let scope = issuer
        .issue("child", &grants, Some(seed.child_agent), None, &mut None)
        .expect("scope proof");
    let reader = FilesystemContentVerifier::new(
        storage.host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        262_144,
        None,
    )?;
    let mut child = StreamAggregate::open(
        &storage.stream,
        (
            seed.child.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let parent_history = parent.history_reader()?;
    let child_history = child.history_reader()?;
    let history_limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 262_144,
    };
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    child
        .bind_published_child(
            parent,
            seed,
            scope.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let capture = seed
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("no Summary capture".into()))?;
    let denied = issuer
        .issue(
            "denied",
            &scope.capabilities().without(&Capabilities::new([
                capture.context.read_capability(&mut None)?,
                seed.child_private_volume
                    .capability(VolumeOperation::Read, &mut None)?,
            ])),
            Some(seed.child_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &denied,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                Limits {
                    file_bytes: 1,
                    ..limits
                },
                history_limits,
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let mut changed = seed.clone();
    changed.parent_revision += 1;
    assert!(
        child_history
            .summary_fork_stage(
                &parent_history,
                &changed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await
            .is_err()
    );
    let other_stream = Arc::new(MemoryStream::default());
    let mut unrelated = StreamAggregate::open(
        &other_stream,
        (
            seed.child.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    unrelated
        .execute(
            Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("independent-child-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: seed.child_agent,
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(matches!(
        unrelated
            .history_reader()?
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let stage = Arc::new(
        assert_cold_summary_reads(
            storage,
            parent,
            seed,
            &issuer,
            &scope,
            reader.clone(),
            limits,
        )
        .await?,
    );
    let model = Arc::new(summary_model(1));
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "summary-child".into(),
            "1".into(),
            Value::Null,
        )?,
        model.clone(),
        ContextPipeline::new([stage as Arc<dyn ContextStage>]),
        ToolRegistry::default(),
    )
    .with_limits(limits)
    // This fixture checks the frozen prefix before any subsequent child compaction.
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
    let journal = FilesystemExecutionJournal::new(
        storage.stream.clone(),
        storage.host.clone(),
        &crate::core::SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &seed.child_private_volume,
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: &scope,
            maximum_bytes: 262_144,
            io: None,
        },
    )?
    .with_input_verifier(reader);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: crate::model::ModelContent::Text("fresh child input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    let completed_requests = model.dispatches().len();
    assert_eq!(executor.execute(input.clone(), &journal).await?, output);
    let mut requests = model.requests();
    assert_eq!(requests.len(), completed_requests);
    assert_eq!(model.remaining(), 0);
    let request = PreparedModelRequest::prepare(
        requests
            .pop()
            .ok_or_else(|| Error::Invalid("child made no model request".into()))?,
        limits,
        None,
    )?;
    let capacity = model.context_capacity(&request.request().model)?;
    let input_tokens = model
        .count_tokens(&request)
        .await?
        .validate(&request, &mut None)?;
    assert!(input_tokens + u64::from(capacity.output_tokens) <= u64::from(capacity.context_tokens));
    assert_eq!(
        request.request().messages.get(..captured.messages.len()),
        Some(captured.messages.as_slice())
    );
    assert_eq!(
        request
            .request()
            .messages
            .last()
            .map(|message| &message.content),
        Some(&input.input)
    );
    Ok(())
}

#[tokio::test]
async fn summary_fork_captures_private_payloads_and_binds_publication() -> Result<()> {
    let early = "q".repeat(60_000);
    let late = "r".repeat(60_000);
    let (storage, parent, seed, captured, limits) = published_summary(
        [early.as_bytes(), early.as_bytes(), late.as_bytes()],
        Arc::new(summary_model(5)),
    )
    .await?;
    assert_summary_child_import(&storage, &parent, &seed, &captured, limits).await
}

#[path = "summary_round_trips.rs"]
mod summary_round_trips;

async fn publish_summary_seed(
    storage: &MemoryHarnessStorage,
    mut aggregate: StreamAggregate<MemoryStream>,
    seed: &crate::fork::ForkSeed,
    scope: Scope,
    stream_provider: &ProviderRef,
) -> Result<StreamAggregate<MemoryStream>> {
    let fs_verifier = FilesystemForkVerifier::new(storage.host.clone(), 262_144, None, None)?;
    aggregate = aggregate.with_fork_verifier(CompositeForkVerifier::new(
        &[
            fs_verifier,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?);
    aggregate
        .execute(
            Command {
                operation_id: seed.operation_id,
                idempotency_key: IdempotencyKey::new("publish-summary-fork")?,
                expected_revision: aggregate.reducer().revision(),
                scope,
                causal_parent: None,
                action: Action::PublishFork {
                    seed: Box::new(seed.clone()),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    Ok(aggregate)
}
