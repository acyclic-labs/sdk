//! Real admitted compaction captured into a child-owned fork environment.
use super::*;
use crate::context::{Context, ModelContextCapacity, ModelTokenCount};
use crate::filesystem::{FilesystemForkPreparer, FilesystemForkVerifier};
use crate::fork::{
    CompositeForkVerifier, ForkHistoryPolicy, ForkPreparation, ForkRequest, ForkSeedVerifier,
    ForkSelection, ResourceRevision, StreamHistoryForkVerifier, SummaryForkSelection,
    prepare_summary_fork_context,
};
use crate::model::{ModelAttempt, ModelDispatch, ModelEvent, PreparedModelRequest};
use crate::resources::StreamRef;
use crate::store::{HistoryReadLimits, HistoryReader};
use std::sync::Mutex;

#[derive(Default)]
struct SummaryModel(Mutex<Vec<PreparedModelRequest>>);

impl ModelProvider for SummaryModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let bytes = message
                    .content
                    .file_refs()
                    .iter()
                    .try_fold(0_u64, |bytes, file| {
                        bytes
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("synthetic accounting overflow".into()))
                    })?;
                u32::try_from(bytes + 512).map_err(|error| Error::Invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ModelTokenCount {
            request_digest: request.manifest().request_digest,
            fixed_tokens: 512,
            message_tokens,
        })
    }
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        assert_eq!(dispatch.request_digest, request.manifest().request_digest);
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "admitted summary or answer".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }
    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture attempts settle immediately".into(),
            ))
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one actual parent compaction, child capture, retry and publication scenario"
)]
async fn summary_fork_captures_private_payloads_and_binds_publication() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 262_144).await?;
    let model = Arc::new(SummaryModel::default());
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let bundle = storage
        .builder()
        .model(
            Model::new("synthetic", "summary-fork", "1", Value::Null)?,
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
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "input.txt",
            )
            .await?;
        storage
            .run_conversation(&bundle, last, input, Vec::new(), 1)
            .await?;
    }
    assert_eq!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        3
    );
    let reference =
        crate::executor::canonical_checkpoint_for_operation(storage.journal.as_ref(), last, limits)
            .await?
            .ok_or_else(|| Error::Invalid("missing admitted checkpoint".into()))?;
    let mut aggregate = StreamAggregate::open(
        &storage.stream,
        storage.conversation.clone(),
        storage.verifier(),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    let history = HistoryReader::new(&storage.stream, &storage.conversation, storage.verifier())?;
    let cursor = history.pin(0).await?;
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
        &RuntimeScope::new(storage.scope.capabilities().clone(), limits)?,
        &history,
        &cursor,
        storage.content_verifier.as_ref(),
    )
    .await?;
    let proof: crate::context::CompactionReference = crate::contract::json_from_slice(
        &storage
            .journal
            .load(&prepared.checkpoint().compaction)
            .await?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(proof.summary.is_some());
    let source = prepared.context().clone();
    let selection = prepared.selection().clone();
    let provider = storage.volume.provider().clone();
    let project = VolumeRef::new(
        provider.clone(),
        "summary-parent-project",
        VolumeClass::Project,
        VolumeOwner::Project("summary-project".into()),
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
                project.capability(VolumeOperation::Read)?,
            ]),
    );
    let scope = storage
        .issuer
        .root_for_agent(agent, "summary-parent", grants);
    let stream_provider = ProviderRef::new("local", "stream", "2")?;
    let preparer = FilesystemForkPreparer::new(
        storage.host.clone(),
        aggregate.reducer().clone(),
        storage.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        storage.content_verifier.clone(),
    )?
    .with_summary_context(prepared, storage.journal())?;
    let child_agent = AgentId::new();
    let child_private = VolumeRef::new(
        provider.clone(),
        "summary-child-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    let request = ForkRequest {
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
            summary: None,
            child_project_volume: VolumeRef::new(
                provider,
                "summary-child-project",
                VolumeClass::Project,
                VolumeOwner::Project("summary-project".into()),
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
                        .resolve(&crate::filesystem::workspace_ref(
                            storage.volume.provider().clone(),
                            &storage.volume.storage_name()?,
                        )?)
                        .await?
                        .generation,
                    paths: Vec::new(),
                },
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(),
                    storage.conversation.stream_path()?.into_bytes(),
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
    )?;
    let report = aggregate.prepare_fork(&preparer, request.clone()).await?;
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
    assert_eq!(report.inherited_through_sequence, 4);
    let bytes = storage.host.read_pinned(&capture.context, 262_144).await?;
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
                hex::encode(crate::contract::canonical_json_digest(file)?),
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
            storage.host.read_pinned(copy, 262_144).await?.as_ref(),
            storage.journal.load(original).await?
        );
    }
    let seed = report.into_seed()?;
    let fs_verifier = Arc::new(FilesystemForkVerifier::new(storage.host.clone(), 262_144)?);
    fs_verifier.verify(&seed).await?;
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
        fs_verifier.verify(&changed).await,
        Err(Error::Conflict(_))
    ));
    aggregate = aggregate.with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
        fs_verifier,
        Arc::new(StreamHistoryForkVerifier::new(stream_provider)?),
    ])?));
    aggregate
        .execute(Command {
            operation_id: seed.operation_id,
            idempotency_key: IdempotencyKey::new("publish-summary-fork")?,
            expected_revision: aggregate.reducer().revision(),
            scope,
            causal_parent: None,
            action: Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        })
        .await?;
    assert_eq!(aggregate.reducer().fork(&seed.child), Some(&seed));
    assert_summary_child_import(&storage, &aggregate, &seed, &captured, limits).await?;
    Ok(())
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
                .capability(VolumeOperation::Write)?,
            seed.child_private_volume
                .capability(VolumeOperation::Read)?,
        ]),
    );
    let scope = issuer.root_for_agent(seed.child_agent, "child", grants);
    let reader = Arc::new(FilesystemContentVerifier::new(
        storage.host.clone(),
        issuer.verifier(),
        scope.clone(),
        262_144,
    )?);
    let mut child = StreamAggregate::open(
        &storage.stream,
        seed.child.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    assert!(matches!(
        child
            .summary_fork_stage(parent, seed, &scope, reader.clone(), limits)
            .await,
        Err(Error::Unauthorized(_))
    ));
    child
        .bind_published_child(parent, seed, scope.clone())
        .await?;
    let capture = seed
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("no Summary capture".into()))?;
    let denied = issuer.root_for_agent(
        seed.child_agent,
        "denied",
        scope.capabilities().without(&Capabilities::new([
            capture.context.read_capability()?,
            seed.child_private_volume
                .capability(VolumeOperation::Read)?,
        ])),
    );
    assert!(matches!(
        child
            .summary_fork_stage(parent, seed, &denied, reader.clone(), limits)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        child
            .summary_fork_stage(
                parent,
                seed,
                &scope,
                reader.clone(),
                Limits {
                    file_bytes: 1,
                    ..limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let mut changed = seed.clone();
    changed.parent_revision += 1;
    assert!(
        child
            .summary_fork_stage(parent, &changed, &scope, reader.clone(), limits)
            .await
            .is_err()
    );
    // A same-agent conversation bound independently is not the admitted fork.
    let other_stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let mut unrelated = StreamAggregate::open(
        &other_stream,
        seed.child.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    unrelated
        .execute(Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new("independent-child-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation {
                agent: seed.child_agent,
            },
        })
        .await?;
    assert!(matches!(
        unrelated
            .summary_fork_stage(parent, seed, &scope, reader.clone(), limits)
            .await,
        Err(Error::Conflict(_))
    ));
    let stage = Arc::new(
        child
            .summary_fork_stage(parent, seed, &scope, reader.clone(), limits)
            .await?,
    );
    let model = Arc::new(SummaryModel::default());
    let executor = StockExecutor::new(
        Model::new("synthetic", "summary-child", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::new([stage as Arc<dyn ContextStage>]),
        ToolRegistry::new(),
    )
    .with_limits(limits);
    let journal = FilesystemExecutionJournal::new(
        storage.stream.clone(),
        storage.host.clone(),
        seed.child_private_volume.clone(),
        issuer.verifier(),
        scope,
        262_144,
    )?
    .with_input_verifier(reader);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: crate::model::ModelContent::Text("fresh child input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(executor.execute(input.clone(), &journal).await?, output);
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
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
