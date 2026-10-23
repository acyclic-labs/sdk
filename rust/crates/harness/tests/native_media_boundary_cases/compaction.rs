use super::*;

pub(super) async fn exercise_media_compaction_fork_restart<A, O>(
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    reader: Arc<ReaderSpy>,
    selected: &[(FileRef, NativeMediaPolicy)],
    (invocation, result): (ToolInvocation, ToolResultContent),
    context: &acyclic_harness::runtime::TaskContext,
    (task_host, task_issuer, task_scope, fence): (
        &Arc<acyclic_harness::durable_host::CoordinatorTaskHost<MemoryStream>>,
        &AuthorityIssuer,
        &Scope,
        &LeaseFence,
    ),
) -> Result<()>
where
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    use acyclic_harness::context::{
        CompactionPolicy, CompactionRetention, Context, ContextPlacement, SourceStage,
        ThresholdCompaction,
    };
    use acyclic_harness::executor::{
        ExecutionEvent, ExecutionJournal, Executor, ModelPurpose, StockExecutor,
    };
    use acyclic_harness::filesystem::{FilesystemForkPreparer, workspace_ref};
    use acyclic_harness::fork::{ForkPreparation, ForkRequest, ForkSelection, ResourceRevision};
    use acyclic_harness::model::ModelPrefix;
    use acyclic_harness::resources::StreamRef;
    use acyclic_harness::store::StreamAggregate;
    let stream = Arc::new(MemoryStream::default());
    let stream_provider =
        ProviderRef::new("media-fork".into(), "stream".into(), "2".into(), &mut None)?;
    let parent = Authority {
        kind: AggregateKind::Conversation,
        id: "media-parent".into(),
    };
    let issuer = AuthorityIssuer::new("media-fork", [42; 32], parent.clone());
    let agent = match volume.owner() {
        VolumeOwner::Agent(agent) => *agent,
        _ => return Err(Error::Invalid("private media volume".into())),
    };
    let project = VolumeRef::new(
        volume.provider().clone(),
        "media-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("media".into()),
        &mut None,
    )?;
    let project_head = host.create_volume(&project).await?;
    let grants = Capabilities::new([
        "conversation:bind".into(),
        "conversation:append".into(),
        "fork:publish".into(),
        "model:generate".into(),
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        project.capability(VolumeOperation::Read, &mut None)?,
        project.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = issuer
        .issue("media-parent", &grants, Some(agent), None, &mut None)
        .expect("scope proof");
    let task = context
        .durable_task_id()
        .ok_or_else(|| Error::Unauthorized("compaction requires its admitted task".into()))?;
    let operation = OperationId::new();
    let journal = FilesystemExecutionJournal::for_task(
        task_host.journal_owner(task, fence, None).await?,
        operation,
        operation,
        host.clone(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &task_issuer.verifier(&mut None)?,
            scope: task_scope,
            maximum_bytes: 65_536,
            io: None,
        },
    )?
    .with_input_verifier(reader.clone());
    let adapter = Arc::new(MockAdapter::new(reader.clone()));
    let model = Model::new(
        "fixture-native".into(),
        "all".into(),
        "mock-native-1".into(),
        json!({"context_tokens":16_384}),
    )?;
    let exchange = vec![
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![ModelContentPart::ToolCall(
                acyclic_harness::model::ToolCall::new(
                    invocation.call_id.clone(),
                    invocation.name.clone(),
                    invocation.arguments,
                ),
            )]),
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Parts(vec![ModelContentPart::ToolResult {
                call_id: invocation.call_id,
                name: invocation.name,
                content: result,
            }]),
        },
    ];
    let mut messages = vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("old history ".repeat(1_100)),
    }];
    messages.extend(exchange.clone());
    let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
        "media-history",
        "1",
        Arc::new(Context {
            messages,
            metadata: BTreeMap::new(),
            current_input_index: None,
        }),
        ContextPlacement::Prepend,
    ))
        as Arc<dyn acyclic_harness::context::ContextStage>]);
    let tool_scope = context.scope().clone();
    let mut tools = ToolRegistry::default();
    tools.register(
        (explicit_conversion::conversion_tool()?, None),
        &mut None,
        None,
    )?;
    let executor = StockExecutor::new(model.clone(), adapter.clone(), pipeline, tools.clone())
        .with_limits(context.scope().limits())
        .with_durable_task(task_host.clone(), task, fence.clone())
        .with_tool_authority(tool_scope.clone(), None)?
        .with_max_output_tokens(16)?
        .with_compaction_policy(CompactionPolicy::Threshold(ThresholdCompaction {
            response_reserve_tokens: 4_096,
            recent_tokens: 1_024,
            retention: CompactionRetention::default(),
        }));
    let input = TurnInput {
        operation_id: operation,
        selected_context: None,
        max_steps: 1,
        input: ModelContent::Parts(
            selected
                .iter()
                .map(|(file, policy)| ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
                })
                .collect(),
        ),
    };
    executor
        .execute_with_context(context, input.clone(), &journal)
        .await?;
    let records = journal.replay(input.operation_id, 0, 64, None)?.await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                ExecutionEvent::ModelStarted {
                    purpose: ModelPurpose::Summary,
                    ..
                }
            ))
            .count(),
        1
    );
    let request = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                purpose: ModelPurpose::Response,
                request,
                ..
            } => Some(request),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("compacted native response".into()))?;
    let admitted = PreparedModelRequest::decode(
        &journal.load(request, None)?.await?,
        Limits::default(),
        None,
    )?;
    assert!(
        admitted
            .request()
            .messages
            .windows(2)
            .any(|messages| messages == exchange)
    );
    assert!(!admitted.request().messages.iter().any(|message| matches!(&message.content, ModelContent::Text(text) if text.starts_with("old history"))));
    let publisher = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        65_536,
        None,
    )?;
    let mut exported = admitted.request().clone();
    for message in &mut exported.messages {
        if let ModelContent::Part(ModelContentPart::File {
            file,
            policy: FileProjectionPolicy::BoundedFull,
        }) = &mut message.content
            && file.path().starts_with(".system/")
        {
            let bytes = journal.load(file, None)?.await?;
            *file = publisher
                .stage(
                    OperationId::new(),
                    "fork/media-summary",
                    &bytes,
                    "text/plain",
                    "media-summary",
                    acyclic_harness::conversation::ContentAccess::default(),
                )?
                .await?
                .0;
        }
    }
    let exported = PreparedModelRequest::prepare(exported, Limits::default(), None)?;
    let prefix = publisher
        .stage(
            operation,
            "fork/media-prefix",
            &ModelPrefix::select(&exported, None)?.canonical_bytes(None)?,
            ModelPrefix::MEDIA_TYPE,
            "media-prefix",
            acyclic_harness::conversation::ContentAccess::default(),
        )?
        .await?
        .0;
    let forks = acyclic_harness::fork::CompositeForkVerifier::new(
        &[
            acyclic_harness::filesystem::FilesystemForkVerifier::new(
                host.clone(),
                65_536,
                None,
                None,
            )?,
            acyclic_harness::fork::StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    let mut aggregate = StreamAggregate::open(
        &stream,
        (
            parent.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(reader.clone())
    .with_fork_verifier(forks.clone());
    for action in [
        Action::BindConversation { agent },
        Action::AppendConversationMessage {
            message: Box::new(acyclic_harness::conversation::ConversationMessage {
                id: uuid::Uuid::new_v4(),
                sequence: 1,
                kind: acyclic_harness::conversation::MessageKind::User,
                content: prefix.clone(),
                attachments: acyclic_harness::conversation::ReferencedAttachments::Inline {
                    items: exported
                        .request()
                        .messages
                        .iter()
                        .flat_map(|message| message.content.file_refs())
                        .cloned()
                        .fold(Vec::new(), |mut files, file| {
                            if !files.contains(&file) {
                                files.push(file);
                            }
                            files
                        })
                        .into_iter()
                        .map(|file| acyclic_harness::conversation::Attachment { file, label: None })
                        .collect::<Vec<_>>(),
                },
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new().into(),
            }),
        },
    ] {
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::new(),
                    idempotency_key: IdempotencyKey::new(format!(
                        "media:{}",
                        aggregate.reducer().revision()
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action,
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let child_agent = AgentId::new();
    let child = Authority {
        kind: AggregateKind::Conversation,
        id: "media-child".into(),
    };
    let child_private = VolumeRef::new(
        volume.provider().clone(),
        "media-child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let child_project = VolumeRef::new(
        volume.provider().clone(),
        "media-child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("media".into()),
        &mut None,
    )?;
    let preparer = FilesystemForkPreparer::new(
        host.clone(),
        Reducer::restore(
            aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            &mut None,
        )?,
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        reader.clone(),
        None,
        None,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?;
    let report = aggregate
        .prepare_fork(
            &preparer,
            ForkRequest {
                authorized_source: None,
                operation_id: OperationId::new(),
                parent: parent.clone(),
                parent_revision: aggregate.reducer().revision(),
                child: child.clone(),
                child_agent,
                attached_agents: Vec::new(),
                boundary: None,
                preparation: ForkPreparation {
                    child_result_volume: None,
                    summary: None,
                    child_project_volume: child_project.clone(),
                    child_private_volume: child_private.clone(),
                    inherited_through_sequence: 1,
                    maximum_inherited_messages: 4,
                    maximum_inherited_bytes: 65_536,
                    maximum_inherited_references: 32,
                },
                selections: vec![
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::History(
                            (StreamRef::new(
                                stream_provider,
                                parent.stream_path(&mut None)?.into_bytes(),
                                Some(aggregate.reducer().revision().to_string()),
                            )?)
                            .into(),
                        ),
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::Project {
                            volume: (project).into(),
                            generation: (project_head.generation).into(),
                        },
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::PrivateVolume {
                            volume: (volume.clone()).into(),
                            generation: (host
                                .resolve(
                                    &workspace_ref(
                                        volume.provider().clone(),
                                        &volume.storage_name(&mut None)?,
                                    )?,
                                    None,
                                )
                                .await?
                                .generation)
                                .into(),
                            paths: Vec::new(),
                        },
                    },
                ],
            },
        )
        .await?;
    let child_issuer = AuthorityIssuer::new("media-fork-child", [43; 32], child.clone());
    let child_scope = child_issuer
        .issue(
            "media-child",
            &Capabilities::new(
                [
                    "conversation:bind".into(),
                    child_private.capability(VolumeOperation::Read, &mut None)?,
                    child_private.capability(VolumeOperation::Write, &mut None)?,
                    child_project.capability(VolumeOperation::Read, &mut None)?,
                    child_project.capability(VolumeOperation::Write, &mut None)?,
                ]
                .into_iter()
                .chain(
                    report
                        .reference_grants
                        .iter()
                        .map(|grant| grant.capability())
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
            Some(child_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let child_reader = FilesystemContentVerifier::new(
        host.clone(),
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &child_scope,
        65_536,
        None,
    )?;
    let mut child_aggregate = StreamAggregate::open(
        &stream,
        (
            child,
            child_issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(child_reader.clone())
    .with_fork_verifier(forks);
    child_aggregate
        .spawn_from_report(&mut aggregate, report, scope, child_scope.clone())
        .await?;
    let child_executor = || {
        StockExecutor::new(
            model.clone(),
            adapter.clone(),
            ContextPipeline::default(),
            tools.clone(),
        )
        .with_limits(context.scope().limits())
        .with_durable_task(task_host.clone(), task, fence.clone())
        .with_tool_authority(tool_scope.clone(), None)?
        .with_compaction_policy(CompactionPolicy::Disabled)
        .with_inherited_prefix(prefix.clone(), child_reader.clone())
    };
    let child_operation = OperationId::new();
    let child_input = TurnInput {
        operation_id: child_operation,
        input: ModelContent::Text("child continues".into()),
        selected_context: None,
        max_steps: 1,
    };
    let open_journal = async || {
        FilesystemExecutionJournal::for_task(
            task_host.journal_owner(task, fence, None).await?,
            child_operation,
            child_operation,
            host.clone(),
            acyclic_harness::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &task_issuer.verifier(&mut None)?,
                scope: task_scope,
                maximum_bytes: 65_536,
                io: None,
            },
        )
        .map(|journal| journal.with_input_verifier(child_reader.clone()))
    };
    let child_output = child_executor()?
        .execute_with_context(context, child_input.clone(), &open_journal().await?)
        .await?;
    let captures = adapter
        .captures
        .lock()
        .map_err(|_| Error::Storage("media fork capture".into()))?
        .clone();
    let parent_capture = captures
        .get(1)
        .ok_or_else(|| Error::NotFound("parent native capture".into()))?;
    let child_capture = captures
        .last()
        .ok_or_else(|| Error::NotFound("child native capture".into()))?;
    let media = |capture: &Capture| {
        capture
            .parts
            .iter()
            .filter_map(|part| match part {
                CapturedPart::Media(media) => Some(media.as_ref().clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(media(parent_capture), media(child_capture));
    drop(child_aggregate);
    drop(aggregate);
    let reopened = open_journal().await?;
    assert_eq!(
        child_executor()?
            .execute_with_context(context, child_input, &reopened)
            .await?,
        child_output
    );
    assert_eq!(
        *adapter
            .captures
            .lock()
            .map_err(|_| Error::Storage("media reopen capture".into()))?,
        captures
    );
    assert_eq!(
        PreparedModelRequest::decode(
            &journal.load(request, None)?.await?,
            Limits::default(),
            None
        )?
        .bytes(),
        admitted.bytes()
    );
    Ok(())
}
