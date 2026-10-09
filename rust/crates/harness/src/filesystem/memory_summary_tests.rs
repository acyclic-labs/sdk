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
    {
        let aggregate = storage.open_conversation(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), 4);
        assert_eq!(conversation.resident_after_sequence(), 3);
        assert_eq!(conversation.messages().len(), 1);
        assert_eq!(conversation.messages()[0].sequence, 4);
    }
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

// Observe the real provider through the public client; every mutation is denied.
struct ColdSummaryStream {
    client: StreamClient<MemoryStream>,
    reads: std::sync::atomic::AtomicUsize,
    maximum: std::sync::atomic::AtomicU32,
    fail_read: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for ColdSummaryStream {
    async fn inspect_idempotency(
        &self,
        _: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.client.stream(path.as_str())?.tail().await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.client.bounds(path.as_str()).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        use std::sync::atomic::Ordering;
        let number = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(request.limit, Ordering::SeqCst);
        if number == self.fail_read.load(Ordering::SeqCst) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.client
            .stream(request.path.as_str())?
            .read(request.from, request.limit)
            .await
    }
    async fn read_commit(
        &self,
        commit: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.client.read_commit(commit).await
    }
    async fn append(
        &self,
        _: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn fork(
        &self,
        _: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn commit(
        &self,
        _: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn follow(
        &self,
        _: acyclic_stream::StreamPath,
        _: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn children(
        &self,
        _: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
}

async fn append_later_summary_parent_messages(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    count: usize,
) -> Result<()> {
    let content = parent
        .reducer()
        .conversation()
        .and_then(|state| state.messages().last())
        .ok_or_else(|| Error::Invalid("missing parent message".into()))?
        .content
        .clone();
    let mut writer = StreamAggregate::open(
        &storage.stream,
        storage.conversation.clone(),
        storage.verifier(),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    for index in 0..count {
        let sequence = writer
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("missing conversation".into()))?
            .messages()
            .len() as u64
            + 1;
        writer
            .execute(Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new(format!("later-summary-parent:{index}"))?,
                expected_revision: writer.reducer().revision(),
                scope: storage.scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::new_v4(),
                        sequence,
                        kind: MessageKind::System,
                        content: content.clone(),
                        attachments: Vec::new().into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            })
            .await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one cold import with exact byte/work bounds, read failure and retained-history scaling"
)]
async fn assert_cold_summary_reads(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    seed: &crate::fork::ForkSeed,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    reader: Arc<dyn ContentResidencyVerifier>,
    limits: Limits,
) -> Result<crate::context::PinnedContextStage> {
    use std::sync::atomic::Ordering;
    let parent_history = parent.history_reader()?;
    let child_history = HistoryReader::new(&storage.stream, &seed.child, issuer.verifier())?;
    let binding = child_history
        .read_page(
            &child_history.pin(0).await?,
            HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: 262_144,
            },
        )
        .await?
        .events
        .into_iter()
        .next()
        .ok_or_else(|| Error::Invalid("missing binding".into()))?;
    let (_, parent_bytes) = parent_history
        .operation_event_bounded(seed.operation_id, 262_144)
        .await?;
    let (_, binding_bytes) = child_history
        .operation_event_bounded(binding.operation_id, 262_144)
        .await?;
    let history_limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: parent_bytes + binding_bytes,
    };
    let mut stage = None;
    for later_messages in [0, 1_000] {
        append_later_summary_parent_messages(storage, parent, later_messages).await?;
        let observed = Arc::new(ColdSummaryStream {
            client: storage.stream.clone(),
            reads: 0.into(),
            maximum: 0.into(),
            fail_read: 0.into(),
        });
        let client = StreamClient::new(observed.clone());
        let cold_parent = HistoryReader::new(&client, &seed.parent, storage.verifier())?;
        let cold_child = HistoryReader::new(&client, &seed.child, issuer.verifier())?;
        if later_messages == 0 {
            for allowance in [
                HistoryReadLimits {
                    maximum_events: 1,
                    ..history_limits
                },
                HistoryReadLimits {
                    maximum_bytes: 0,
                    ..history_limits
                },
            ] {
                assert!(matches!(
                    cold_child
                        .summary_fork_stage(
                            &cold_parent,
                            seed,
                            scope,
                            reader.clone(),
                            limits,
                            allowance
                        )
                        .await,
                    Err(Error::Invalid(_))
                ));
                assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
            }
            assert!(matches!(
                cold_child
                    .summary_fork_stage(
                        &cold_parent,
                        seed,
                        scope,
                        reader.clone(),
                        limits,
                        HistoryReadLimits {
                            maximum_bytes: history_limits.maximum_bytes - 1,
                            ..history_limits
                        }
                    )
                    .await,
                Err(Error::Invalid(_))
            ));
            assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
            observed.reads.store(0, Ordering::SeqCst);
            observed.fail_read.store(3, Ordering::SeqCst);
            assert!(
                cold_child
                    .summary_fork_stage(
                        &cold_parent,
                        seed,
                        scope,
                        reader.clone(),
                        limits,
                        history_limits
                    )
                    .await
                    .is_err()
            );
            assert_eq!(observed.reads.load(Ordering::SeqCst), 3);
            observed.reads.store(0, Ordering::SeqCst);
            observed.fail_read.store(0, Ordering::SeqCst);
        }
        // Fresh reader objects after failed observation: no restored reducer or cache.
        let cold_parent = HistoryReader::new(&client, &seed.parent, storage.verifier())?;
        let cold_child = HistoryReader::new(&client, &seed.child, issuer.verifier())?;
        stage = Some(
            cold_child
                .summary_fork_stage(
                    &cold_parent,
                    seed,
                    scope,
                    reader.clone(),
                    limits,
                    history_limits,
                )
                .await?,
        );
        assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
        assert_eq!(observed.maximum.load(Ordering::SeqCst), 1);
    }
    stage.ok_or_else(|| Error::Invalid("cold Summary fixture did not run".into()))
}

#[tokio::test]
async fn default_compaction_bounds_small_message_continuation() -> Result<()> {
    let mut storage = MemoryHarnessStorage::new(AgentId::new(), 262_144).await?;
    let model = Arc::new(SummaryModel::default());
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let build_bundle = |storage: &MemoryHarnessStorage| -> Result<crate::bundle::HarnessBundle> {
        storage
            .builder()
            .model(
                Model::new("synthetic", "small-message-continuation", "1", Value::Null)?,
                model.clone(),
            )
            .grant("model:generate")
            .limits(limits)
            .build()
    };
    let mut bundle = build_bundle(&storage)?;
    let mut original = None;
    for index in 0..20 {
        let operation = OperationId::new();
        let input = storage
            .stage(
                operation,
                &format!("turns/{index}/input.txt"),
                b"small input",
                "text/plain",
                "input.txt",
            )
            .await?;
        let output = storage
            .run_conversation(&bundle, operation, input.clone(), Vec::new(), 1)
            .await?;
        if original.is_none() {
            original = Some((operation, input, output));
        }
        let aggregate = storage.open_conversation(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), (index + 1) * 2);
        assert!(conversation.messages().len() <= limits.context_messages);
        if index >= 3 {
            assert!(conversation.resident_after_sequence() > 0);
        }
        drop(aggregate);
        if index == 17 {
            // Reconstruct the real journal and bundle with original authority
            // and providers. No original projection or execution cache survives.
            storage.journal = Arc::new(
                FilesystemExecutionJournal::new(
                    storage.stream.clone(),
                    storage.host.clone(),
                    storage.volume.clone(),
                    storage.verifier(),
                    storage.scope.clone(),
                    storage.maximum_file_bytes,
                )?
                .with_input_verifier(storage.content_verifier.clone()),
            );
            bundle = build_bundle(&storage)?;
        }
    }
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len();
    assert!(requests > 20, "count pressure must admit summary requests");
    let (operation, input, expected) =
        original.ok_or_else(|| Error::Invalid("missing original turn".into()))?;
    assert_eq!(
        storage
            .run_conversation(&bundle, operation, input, Vec::new(), 1)
            .await?,
        expected
    );
    assert_eq!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        requests
    );
    Ok(())
}
