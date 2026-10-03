//! Exact model boundaries passed through real durable workspace/conversation forks.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    batch_publication::{ModelBatchPublication, ModelBatchPublisher},
    conversation::{
        ConversationMessage, Limits, MessageKind, ModelContextSelection, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, SchemaRegistry,
    },
    executor::ExecutionEvent,
    filesystem::{
        DurableHarnessStorage, FilesystemContentVerifier, FilesystemForkPreparer,
        FilesystemForkVerifier, FilesystemHost, FilesystemProjectMergeVerifier, HarnessStorage,
        workspace_ref,
    },
    fork::{
        CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection, ResourceRevision,
        StreamHistoryForkVerifier,
    },
    model::{
        FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
        ModelMessage, ModelProvider, ModelRequest, ModelRole,
    },
    model_input::{CompletedModelBoundary, FrozenModelPrefix, PreparedModelInput},
    projection::select_model_context,
    registry::ComponentIdentity,
    resources::{ProviderRef, StreamRef},
    store::StreamAggregate,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

struct CapturedModel {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    root: bool,
}
impl ModelProvider for CapturedModel {
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.requests.lock().unwrap().push(request);
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let events = if self.root && first {
            vec![
                ModelEvent::Content {
                    delta: " \nα🦀\t retained\n".into(),
                },
                ModelEvent::ToolCall {
                    call_id: "invalid".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({"parameters": {"text": "wrong"}}),
                },
                ModelEvent::ToolCall {
                    call_id: "edit".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({"path":"notes/real.txt","text":"real content","media_type":"text/plain","display_name":"real.txt"}),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        } else {
            vec![
                ModelEvent::Content {
                    delta: "final only".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

struct ForkAtBatch {
    storage: Arc<DurableHarnessStorage>,
    host: Arc<Host>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
    issuer: AuthorityIssuer,
    stream_provider: ProviderRef,
    children: Arc<CapturedModel>,
    limits: Limits,
    publications: AtomicUsize,
    paused: bool,
}
impl ForkAtBatch {
    async fn publish_children(&self, admission: ModelBatchPublication) -> Result<()> {
        let journal = self.storage.journal();
        let bytes = journal.load(&admission.boundary).await?;
        admission.boundary.descriptor().verify(&bytes)?;
        let boundary: CompletedModelBoundary =
            serde_json::from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))?;
        boundary.verify(self.limits)?;
        let mut parent = self
            .storage
            .completed_conversation(admission.parent_operation, admission.step, self.limits)
            .await?;
        let state = parent.reducer().conversation().unwrap();
        let selected = select_model_context(
            state,
            ModelContextSelection {
                conversation_revision: state.messages.len() as u64,
                message_ids: state.messages.iter().map(|message| message.id).collect(),
            },
            &FilesystemContentVerifier::new(
                self.host.clone(),
                self.issuer.verifier(),
                self.storage.owner_scope().clone(),
                self.limits.file_bytes,
            )?,
            self.limits.context_messages,
            self.limits.attachments,
            self.limits.render_bytes,
        )
        .await?;
        assert_eq!(selected.messages, boundary.request.messages);
        let provider = self.project.provider().clone();
        let parent_scope = self.issuer.root_for_agent(
            self.storage.owner_scope().agent().unwrap(),
            "parent-publishing",
            Capabilities::new([
                "conversation:append".to_owned(),
                "fork:publish".to_owned(),
                self.project.capability(VolumeOperation::Read)?,
                self.project.capability(VolumeOperation::Write)?,
                self.storage.volume().capability(VolumeOperation::Read)?,
            ]),
        );
        let forks = Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(
                self.host.clone(),
                self.limits.file_bytes,
            )?),
            Arc::new(StreamHistoryForkVerifier::new(
                self.stream_provider.clone(),
            )?),
        ])?);
        parent = parent.with_fork_verifier(forks.clone());
        let project_head = self.host.create_volume(&self.project).await?;
        for index in 0..2_u8 {
            let child_agent = AgentId::from_bytes([index + 20; 16]);
            let child_authority = Authority {
                kind: AggregateKind::Conversation,
                id: format!("child-{index}"),
            };
            let child_issuer =
                AuthorityIssuer::new("model-fork-e2e", [7; 32], child_authority.clone());
            let private = VolumeRef::new(
                provider.clone(),
                format!("private-{index}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
            )?;
            let project = VolumeRef::new(
                provider.clone(),
                format!("project-{index}"),
                VolumeClass::Project,
                self.project.owner().clone(),
            )?;
            let child_scope = child_issuer.root_for_agent(
                child_agent,
                "child",
                Capabilities::new([
                    "conversation:bind".to_owned(),
                    "conversation:append".to_owned(),
                    private.capability(VolumeOperation::Read)?,
                    private.capability(VolumeOperation::Write)?,
                ]),
            );
            let resolver = Arc::new(FilesystemContentVerifier::new(
                self.host.clone(),
                self.issuer.verifier(),
                parent_scope.clone(),
                self.limits.file_bytes,
            )?);
            let preparer = FilesystemForkPreparer::new(
                self.host.clone(),
                parent.reducer().clone(),
                self.issuer.verifier(),
                parent_scope.clone(),
                self.project.clone(),
                self.stream_provider.clone(),
                resolver,
            )?;
            let request = ForkRequest {
                operation_id: OperationId::from_bytes([index + 40; 16]),
                parent: parent.reducer().authority().clone(),
                parent_revision: parent.reducer().revision(),
                child: child_authority.clone(),
                child_agent,
                attached_agents: Vec::new(),
                preparation: ForkPreparation {
                    child_project_volume: project,
                    child_private_volume: private.clone(),
                    inherited_through_sequence: parent
                        .reducer()
                        .conversation()
                        .unwrap()
                        .messages
                        .len() as u64,
                    maximum_inherited_messages: 64,
                    maximum_inherited_bytes: self.limits.file_bytes,
                    maximum_inherited_references: 128,
                },
                selections: vec![
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::History(StreamRef::new(
                            self.stream_provider.clone(),
                            parent.reducer().authority().stream_path()?.into_bytes(),
                            Some(parent.reducer().revision().to_string()),
                        )?),
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::Project {
                            volume: self.project.clone(),
                            generation: project_head.generation.clone(),
                        },
                    },
                ],
                boundary: None,
            };
            let report = parent.prepare_fork(&preparer, request).await?;
            let mut child = StreamAggregate::open(
                &self.stream,
                child_authority.clone(),
                child_issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?
            .with_fork_verifier(forks.clone())
            .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
                self.host.clone(),
                child_issuer.verifier(),
                child_scope.clone(),
                self.limits.file_bytes,
            )?))
            .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(
                self.host.clone(),
            )));
            child
                .spawn_from_report(&mut parent, report, parent_scope.clone(), child_scope)
                .await?;
            let storage = HarnessStorage::from_providers(
                child_agent,
                self.limits.file_bytes,
                self.host.clone(),
                self.stream.clone(),
                private,
                child_authority,
                child_issuer,
            )
            .await?;
            let suffix = vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "fork child {index}; task: verify; workspace: project-{index}; fresh scratch"
                )),
            }];
            let bundle = storage
                .inherited_builder(boundary.clone(), suffix, self.children.clone(), self.limits)?
                .tools(storage.default_tools(self.limits)?)
                .grant("tool:call:acyclic.read_file")
                .grant("tool:call:acyclic.stage_file")
                .grant("tool:call:acyclic.list_files")
                .limits(self.limits)
                .build()?;
            storage.run_prompt(&bundle, "explicit child input").await?;
        }
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl ModelBatchPublisher for ForkAtBatch {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.filesystem-model-fork".into(),
            version: "1".into(),
            digest: [6; 32],
        }
    }
    fn guarantee(&self) -> EffectGuarantee {
        EffectGuarantee::AtMostOnce
    }
    fn publish<'a>(&'a self, request: ModelBatchPublication) -> BoxFuture<'a, Result<()>> {
        if self.paused {
            return Box::pin(async { Err(Error::Storage("publication interrupted".into())) });
        }
        Box::pin(self.publish_children(request))
    }
    fn reconcile<'a>(&'a self, _: ModelBatchPublication) -> BoxFuture<'a, Result<Option<()>>> {
        Box::pin(async { Ok((self.publications.load(Ordering::SeqCst) > 0).then_some(())) })
    }
}

#[tokio::test]
async fn native_forks_capture_completed_authoritative_exchange_and_exact_model_prefix() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("model-fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("model-fork-e2e", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("fs")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("streams"),
            LocalStreamLimits::default(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("model-fork-e2e", [7; 32], authority.clone());
    let private = VolumeRef::new(
        provider.clone(),
        "root-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    host.create_volume(&private).await?;
    host.create_volume(&project).await?;
    let limits = Limits::default();
    let storage = Arc::new(
        HarnessStorage::from_providers(
            agent,
            limits.file_bytes,
            host.clone(),
            stream.clone(),
            private,
            authority,
            issuer.clone(),
        )
        .await?,
    );
    let model = Model::new("test", "frozen", "1", Value::Null)?;
    let root_model = Arc::new(CapturedModel {
        root: true,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let children = Arc::new(CapturedModel {
        root: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let publisher = Arc::new(ForkAtBatch {
        storage: storage.clone(),
        host,
        stream,
        project,
        issuer,
        stream_provider,
        children: children.clone(),
        limits,
        publications: AtomicUsize::new(0),
        paused: false,
    });
    let bundle = storage
        .builder()
        .model(model, root_model.clone())
        .grant("model:generate")
        .tools(storage.default_tools(limits)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.stage_file")
        .grant("tool:call:acyclic.list_files")
        .batch_publisher(publisher.clone())
        .limits(limits)
        .build()?;
    let operation = OperationId::from_bytes([2; 16]);
    let input = storage
        .stage(
            operation,
            "input/root.txt",
            b"root request",
            "text/plain",
            "root.txt",
        )
        .await?;
    let output = storage
        .run_conversation(&bundle, operation, input.clone(), Vec::new(), 3)
        .await?;
    assert_eq!(output.text, " \nα🦀\t retained\nfinal only");
    assert_eq!(publisher.publications.load(Ordering::SeqCst), 1);
    let boundary = storage
        .completed_model_boundary(operation, 0, limits)
        .await?
        .unwrap();
    let captured = children.requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    for request in captured.iter() {
        assert_eq!(
            &request.messages[..boundary.request.messages.len()],
            boundary.request.messages
        );
        assert_eq!(request.messages.len(), boundary.request.messages.len() + 2);
        let actual = PreparedModelInput::prepare(request.clone(), limits)?;
        let inherited = FrozenModelPrefix::capture(&actual, boundary.request.messages.len())?;
        assert_eq!(inherited.message_bytes(), boundary.prefix.message_bytes());
    }
    drop(captured);
    storage
        .run_conversation(&bundle, operation, input, Vec::new(), 3)
        .await?;
    assert_eq!(publisher.publications.load(Ordering::SeqCst), 1);
    let next_operation = OperationId::from_bytes([3; 16]);
    let next_input = storage
        .stage(
            next_operation,
            "input/follow-up.txt",
            b"follow-up",
            "text/plain",
            "follow-up.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, next_operation, next_input.clone(), Vec::new(), 3)
        .await?;
    let requests = root_model.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let mut expected = boundary.request.messages.clone();
    expected.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text("final only".into()),
    });
    expected.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Parts(vec![ModelContentPart::File {
            file: next_input,
            policy: FileProjectionPolicy::BoundedFull,
        }]),
    });
    assert_eq!(requests[2].messages, expected);
    drop(requests);
    assert!(matches!(
        storage.completed_conversation(operation, 0, limits).await,
        Err(Error::Conflict(_))
    ));
    let records = storage.journal().replay(operation).await?;
    assert!(records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::BatchPublicationCompleted { .. }
    )));
    Ok(())
}

#[tokio::test]
async fn stale_completed_boundary_is_refused_before_publication_files_are_written() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("model-fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("model-fork-e2e", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("fs")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("streams"),
            LocalStreamLimits::default(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("model-fork-e2e", [7; 32], authority.clone());
    let private = VolumeRef::new(
        provider.clone(),
        "root-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    host.create_volume(&private).await?;
    host.create_volume(&project).await?;
    let limits = Limits::default();
    let storage = Arc::new(
        HarnessStorage::from_providers(
            agent,
            limits.file_bytes,
            host.clone(),
            stream.clone(),
            private,
            authority,
            issuer.clone(),
        )
        .await?,
    );
    let model = Model::new("test", "frozen", "1", Value::Null)?;
    let root_model = Arc::new(CapturedModel {
        root: true,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let children = Arc::new(CapturedModel {
        root: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let publisher = Arc::new(ForkAtBatch {
        storage: storage.clone(),
        host: host.clone(),
        stream: stream.clone(),
        project,
        issuer: issuer.clone(),
        stream_provider,
        children: children.clone(),
        limits,
        publications: AtomicUsize::new(0),
        paused: true,
    });
    let bundle = storage
        .builder()
        .model(model, root_model.clone())
        .grant("model:generate")
        .tools(storage.default_tools(limits)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.stage_file")
        .grant("tool:call:acyclic.list_files")
        .batch_publisher(publisher.clone())
        .limits(limits)
        .build()?;

    let operation = OperationId::from_bytes([2; 16]);
    let input = storage
        .stage(
            operation,
            "input/root.txt",
            b"root request",
            "text/plain",
            "root.txt",
        )
        .await?;
    assert!(
        matches!(storage.run_conversation(&bundle, operation, input, Vec::new(), 3).await,
        Err(Error::Storage(message)) if message == "publication interrupted")
    );
    let concurrent = OperationId::from_bytes([4; 16]);
    let content = storage
        .stage(
            concurrent,
            "input/concurrent.txt",
            b"concurrent user input",
            "text/plain",
            "concurrent.txt",
        )
        .await?;
    let authority = storage.conversation().clone();
    let mut aggregate =
        StreamAggregate::open(&stream, authority, issuer.verifier(), SchemaRegistry::new())
            .await?
            .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
                host.clone(),
                issuer.verifier(),
                storage.owner_scope().clone(),
                limits.file_bytes,
            )?));
    let message = ConversationMessage {
        id: uuid::Uuid::from_bytes([4; 16]),
        sequence: aggregate.reducer().conversation().unwrap().messages.len() as u64 + 1,
        kind: MessageKind::User,
        content,
        attachments: Vec::new().into(),
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    aggregate
        .execute(Command {
            operation_id: concurrent,
            idempotency_key: IdempotencyKey::new("concurrent-message")?,
            expected_revision: aggregate.reducer().revision(),
            scope: storage.owner_scope().clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(message),
            },
        })
        .await?;
    let private = workspace_ref(
        storage.volume().provider().clone(),
        &storage.volume().storage_name()?,
    )?;
    let before = host.resolve(&private).await?.generation;
    for _ in 0..2 {
        assert!(matches!(
            storage.completed_conversation(operation, 0, limits).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(host.resolve(&private).await?.generation, before);
    }
    Ok(())
}
