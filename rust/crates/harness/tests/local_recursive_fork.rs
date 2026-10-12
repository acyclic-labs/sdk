//! Bounded durable-local recursive forks across Stream and Filesystem.
#![cfg(feature = "filesystem-local")]
#![allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
use acyclic_harness::context::ContextPipeline;
use acyclic_harness::conversation::{
    Attachment, ContentGrant, ConversationMessage, MessageKind, ReferencedAttachments, VolumeClass,
    VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::conversation::{FileRef, Limits};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry,
};
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier, FilesystemHost,
    FilesystemProjectMergeVerifier, FilesystemProjectWorkspaces, WorkspaceMutation, workspace_ref,
};
use acyclic_harness::filesystem::{FilesystemExecutionJournal, ParentProjectController};
use acyclic_harness::fork::{
    CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection, ResourceRevision,
    StreamHistoryForkVerifier,
};
use acyclic_harness::merge::{ProjectJoinOutcome, ProjectWorkspaceProvider};
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent,
    ModelMessage, ModelPrefix, ModelProvider, ModelRequest, ModelRole, PreparedModelRequest,
};
use acyclic_harness::resources::{ProviderRef, StreamRef};
use acyclic_harness::runtime::RuntimeScope;
use acyclic_harness::store::StreamAggregate;
use acyclic_harness::tool::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry, ToolResult,
};
use acyclic_harness::{AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use futures::{future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const DEPTH: u8 = 3;

impl ToolExecutor for PrefixModel {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
impl ToolProjection for PrefixModel {
    fn project(&self, _: &ToolInvocation, result: &ToolResult,
        work: acyclic_harness::NativeWork<'_>) -> Result<(Value, acyclic_harness::NativeCustody)> {
        self.supports_native_projection(work)?;
        Ok((serde_json::json!({"kind":"json","value":result.value}), None))
    }
}
struct PrefixModel {
    requests: Mutex<Vec<(PreparedModelRequest, Result<bool>)>>,
    children: Value,
    inherited: Option<ModelRequest>,
}
impl PrefixModel {
    fn reuse_prefix(warm: Option<&ModelRequest>, request: &ModelRequest) -> Result<bool> {
        let Some(warm) = warm else { return Ok(false) };
        if warm.model.name != request.model.name {
            return Err(Error::Unsupported(
                "fixture cannot reuse another model profile".into(),
            ));
        }
        Ok(warm.model == request.model && request.messages.starts_with(&warm.messages))
    }
}
impl ModelProvider for PrefixModel {
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        _dispatch: acyclic_harness::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reuse = Self::reuse_prefix(
            requests.last().map(|(request, _)| request.request()).or(self.inherited.as_ref()),
            request.request(),
        );
        requests.push((request, reuse));
        let mut events = Vec::new();
        if requests.len() == 1 {
            events.push(Ok(ModelEvent::ToolCall {
                call_id: "fork-plan".into(),
                name: "example.fork".into(),
                arguments: self.children.clone(),
            }));
        }
        events.push(Ok(ModelEvent::Completed {
            metadata: Value::Null,
        }));
        Box::pin(futures::stream::iter(events))
    }

}

#[allow(
    clippy::too_many_arguments,
    reason = "each fork level threads the same fixtures"
)]
async fn model_fork_prefix<A, O>(
    host: Arc<FilesystemHost<A, O>>,
    stream: &std::sync::Arc<LocalStream>,
    aggregate: &StreamAggregate<LocalStream>,
    issuer: &AuthorityIssuer,
    local_scope: &acyclic_harness::core::Scope,
    private: &VolumeRef,
    project: &VolumeRef,
    previous: Option<(&FileRef, &PreparedModelRequest)>,
    approved: &[FileRef],
    attachment: &FileRef,
    level: u8,
) -> Result<(FileRef, PreparedModelRequest, FileRef)>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let agent = local_scope
        .agent()
        .ok_or_else(|| Error::Invalid("missing parent agent".into()))?;
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        local_scope,
        private,
        VolumeOperation::Write,
    )?;
    let scratch = host
        .put_content(
            private,
            &write,
            "scratch/current.txt",
            format!("fresh scratch {level}").as_bytes(),
            "text/plain",
            "scratch.txt",
            1024,
            &IdempotencyKey::new(format!("scratch-{level}"))?,
         None, None)
        .await?;
    let mut capabilities = local_scope
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for file in approved {
        capabilities.push(file.read_capability()?);
    }
    let read_scope = issuer.issue("prefix-owner-approved-reader", &Capabilities::new(capabilities), Some(agent), None, &mut None).expect("scope proof");
    let reader = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &read_scope, 64 * 1024, None)?;
    let journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &private,
        &issuer.verifier(&mut None).expect("verifier copy"), &acyclic_harness::core::SchemaRegistry::new(),
        &read_scope,
        64 * 1024, None,
    )?
    .with_input_verifier(reader.clone());
    let children = if level == 0 {
        json!({"children": ["local-child-1", "local-sibling"]})
    } else {
        json!({"children": [format!("local-child-{}", level + 1)]})
    };
    let provider = Arc::new(PrefixModel {
        requests: Mutex::new(Vec::new()),
        children,
        inherited: previous.map(|(_, parent)| parent.request().clone()),
    });
    let model = Model::new("example".into(), if level >= 3 { "alternate" } else { "exact" }.into(),
        if level == 2 { "2" } else { "1" }.into(), Value::Null)?;
    let mut tools = ToolRegistry::default();
    tools.register(Tool { definition: ToolDefinition { name: "example.fork".into(), revision: "1".into(), description: "Plan child".into(),
        input_schema: json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}),
        output_schema: json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false})) },
        executor: provider.clone(), projection: provider.clone() })?;
    let mut executor = StockExecutor::new(
        model,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.fork"]),
            Limits::default(),
        )?,
        None,
    )?;
    if let Some((head, _)) = previous {
        executor = executor.with_inherited_prefix(head.clone(), reader.clone())?;
    }
    let input = TurnInput {
        operation_id: OperationId::from_bytes(identity(170 + level)),
        selected_context: None,
        max_steps: 2,
        input: ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: format!(
                    "notification {level}\r\ntask é\0🦀\r\nidentity {agent:?}\r\nworkspace {}",
                    project.id()
                ),
            },
            ModelContentPart::File {
                file: scratch.clone(),
                policy: FileProjectionPolicy::Reference,
            },
            ModelContentPart::File {
                file: attachment.clone(),
                policy: FileProjectionPolicy::Reference,
            },
        ]),
    };
    let controller = ParentProjectController::new(
        &host,
        aggregate.reducer(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        local_scope,
        &project, None,
    )?;
    let private_workspace = workspace_ref(private.provider().clone(), &private.storage_name()?)?;
    let before = host.resolve(&private_workspace).await?;
    assert!(
        controller
            .stage_completed_model_prefix(
                private,
                &executor,
                &journal,
                &input,
                0,
                "fork-plan",
                previous
            )
            .await
            .is_err()
    );
    assert_eq!(
        before.generation,
        host.resolve(&private_workspace).await?.generation
    );
    executor
        .execute(input.clone(), &journal)
        .await
        .map_err(|error| Error::Storage(format!("prefix execute level {level}: {error}")))?;
    let completed = executor
        .completed_tool_prefix(&journal, &input, 0, "fork-plan")
        .await
        .map_err(|error| Error::Storage(format!("prefix extract level {level}: {error}")))?;
    let captured = provider
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter().map(|(request, _)| request.bytes().to_vec()).collect::<Vec<_>>();
    let mut expected = completed.request().clone();
    expected.messages =
        previous.map_or_else(Vec::new, |(_, parent)| parent.request().messages.clone());
    expected.messages.push(ModelMessage {
        role: ModelRole::User,
        content: input.input.clone(),
    });
    assert_eq!(
        captured.first().map(Vec::as_slice),
        Some(PreparedModelRequest::prepare(expected, Limits::default(), None)?.bytes())
    );
    assert_eq!(captured.get(1).map(Vec::as_slice), Some(completed.bytes()));
    drop(captured);
    let cold_journal = FilesystemExecutionJournal::new(
        stream.clone(), host.clone(), &private, &issuer.verifier(&mut None).expect("verifier copy"),
        &SchemaRegistry::new(), &read_scope, 64 * 1024, None,
    )?.with_input_verifier(reader);
    executor.execute(input.clone(), &cold_journal).await?;
    let expected_reuse = match level {
        0 => Some(false),
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };
    {
        let requests = provider.requests.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match expected_reuse {
            Some(expected) => assert!(matches!(requests.first(), Some((_, Ok(reuse))) if *reuse == expected)),
            None => assert!(matches!(requests.first(), Some((_, Err(Error::Unsupported(_)))))),
        }
        assert!(matches!(requests.get(1), Some((_, Ok(true)))));
    }
    assert_eq!(
        provider
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        2
    );
    let segment = ModelPrefix::select(&completed, previous)?;
    let wire: Value = serde_json::from_slice(&segment.canonical_bytes(None)?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        wire.get("messages").and_then(Value::as_array).map(Vec::len),
        Some(3)
    );
    let (head, frozen) = controller
        .stage_completed_model_prefix(
            private,
            &executor,
            &journal,
            &input,
            0,
            "fork-plan",
            previous,
        )
        .await
        .map_err(|error| Error::Storage(format!("prefix stage level {level}: {error}")))?;
    assert_eq!(frozen.bytes(), completed.bytes());
    assert_eq!(
        head,
        controller.stage_model_prefix(private, &segment).await?
    );
    Ok((head, completed, scratch))
}

fn identity(value: u8) -> [u8; 16] {
    let mut bytes = [0xA5; 16];
    bytes[0] = value;
    bytes
}

fn volume(
    provider: &ProviderRef,
    class: VolumeClass,
    id: String,
    owner: VolumeOwner,
) -> Result<VolumeRef> {
    VolumeRef::new(provider.clone(), id, class, owner)
}

fn scope(
    issuer: &AuthorityIssuer,
    agent: AgentId,
    private: &VolumeRef,
    project: &VolumeRef,
) -> Result<acyclic_harness::core::Scope> {
    let next = project
        .id()
        .rsplit('-')
        .next()
        .and_then(|id| id.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    let selected = VolumeRef::new(
        project.provider().clone(),
        format!(
            "{}-{next}",
            if project.id().starts_with("deep-") {
                "deep-project"
            } else {
                "project"
            }
        ),
        VolumeClass::Project,
        project.owner().clone(),
    )?;
    let source_reads = [selected.capability(VolumeOperation::Read)?];
    Ok(issuer.issue(&format!("agent-{agent:?}"), &Capabilities::new(
            [
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "fork:publish".to_owned(),
                "project:merge".to_owned(),
                project.capability(VolumeOperation::Read)?,
                project.capability(VolumeOperation::Write)?,
                private.capability(VolumeOperation::Read)?,
                private.capability(VolumeOperation::Write)?,
            ]
            .into_iter()
            .chain(source_reads),
        ), Some(agent), None, &mut None).expect("scope proof"))
}

fn command(
    id: u8,
    revision: u64,
    scope: &acyclic_harness::core::Scope,
    action: Action,
) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes(identity(id)),
        idempotency_key: IdempotencyKey::new(format!("local-recursive-{id}-{revision}"))?,
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    })
}

async fn open_aggregate<A, O>(
    stream: &std::sync::Arc<LocalStream>,
    authority: Authority,
    issuer: &AuthorityIssuer,
    host: Arc<FilesystemHost<A, O>>,
    scope: acyclic_harness::core::Scope,
    stream_provider: ProviderRef,
) -> Result<StreamAggregate<LocalStream>>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let reader = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &scope, 64 * 1_024, None)?;
    let forks = CompositeForkVerifier::new(&[
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
        StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
    ], &mut None)?;
    Ok(
        StreamAggregate::open(stream, authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), None, acyclic_harness::store::default_projection_read_limits(), acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?
            .with_content_verifier(reader)
            .with_fork_verifier(forks)
            .with_merge_verifier(FilesystemProjectMergeVerifier::new(host, None)?),
    )
}

#[tokio::test]
async fn local_recursive_parent_forks_reopen_and_merge_project_only() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("local-recursive-e2e".into(), "filesystem".into(), "2".into(), &mut None)?;
    let stream_provider = ProviderRef::new("local-recursive-e2e".into(), "stream".into(), "2".into(), &mut None)?;
    let mut host = FilesystemHost::new(
        Fs::local(fs_options.clone())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        &provider, None,
    )?;
    let mut stream = Arc::new(
        LocalStream::open(&stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );

    let root_authority = Authority {
        kind: AggregateKind::Conversation,
        id: "local-root".into(),
    };
    let root_agent = AgentId::from_bytes([1; 16]);
    let root_issuer = AuthorityIssuer::new("local-recursive-e2e", [7; 32], root_authority.clone());
    let root_private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "private-0".into(),
        VolumeOwner::Agent(root_agent),
    )?;
    let root_project = volume(
        &provider,
        VolumeClass::Project,
        "project-0".into(),
        VolumeOwner::Project("local-project".into()),
    )?;
    let root_scope = scope(&root_issuer, root_agent, &root_private, &root_project)?;
    let mut project = root_project.clone();
    let mut private = root_private.clone();
    let mut authority = root_authority.clone();
    let mut issuer = root_issuer.clone();
    let mut grant_scope = root_scope.clone();
    let mut project_head = host.create_volume(&root_project).await?;
    host.create_volume(&root_private).await?;
    let mut aggregate = open_aggregate(
        &stream,
        authority.clone(),
        &issuer,
        host.clone(),
        grant_scope.clone(),
        stream_provider.clone(),
    )
    .await?;
    aggregate
        .execute(command(
            1,
            0,
            &grant_scope,
            Action::BindConversation { agent: root_agent },
        )?, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &private,
        VolumeOperation::Write,
    )?;
    let root_file = host
        .put_content(
            &private,
            &root_write,
            "messages/root.txt",
            b"root message",
            "text/plain",
            "root.txt",
            1_024,
            &IdempotencyKey::new("root-message")?,
         None, None)
        .await?;
    let root_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/root.bin",
            b"root attachment",
            "application/octet-stream",
            "root.bin",
            1_024,
            &IdempotencyKey::new("root-attachment")?,
         None, None)
        .await?;
    let mut previous_attachment = root_attachment.clone();
    let (mut model_head, mut model_request, root_scratch) = model_fork_prefix(
        host.clone(),
        &stream,
        &aggregate,
        &issuer,
        &grant_scope,
        &private,
        &project,
        None,
        &[],
        &root_attachment,
        0,
    )
    .await?;
    let mut approved_model_files = vec![model_head.clone(), root_scratch, root_attachment.clone()];
    aggregate
        .execute(command(
            2,
            1,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([2; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: root_file.clone(),
                    attachments: vec![Attachment {
                        file: root_attachment.clone(),
                        label: None,
                    }]
                    .into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: [("example.model-prefix".into(), model_head.clone())].into(),
                }),
            },
        )?, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;

    let mut final_child_issuer = None;
    let mut final_child_scope = None;
    let mut final_child_private = None;
    let mut final_child_file = None;
    let mut final_child_attachment = None;
    let mut merge_parent_authority = None;
    let mut merge_parent_issuer = None;
    let mut merge_parent_scope = None;
    let mut merge_parent_project = None;
    let mut merge_child_authority = None;

    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes([level + 1; 16]);
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("local-child-{level}"),
        };
        let child_issuer =
            AuthorityIssuer::new("local-recursive-e2e", [7; 32], child_authority.clone());
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            format!("private-{level}"),
            VolumeOwner::Agent(child_agent),
        )?;
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("project-{level}"),
            VolumeOwner::Project("local-project".into()),
        )?;
        let child_scope = scope(&child_issuer, child_agent, &child_private, &child_project)?;
        let parent_reader = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &grant_scope, 64 * 1_024, None)?;
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            aggregate.reducer().clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            grant_scope.clone(),
            project.clone(),
            stream_provider.clone(),
            parent_reader,
            None, None, #[cfg(not(target_arch = "wasm32"))] None,
        )?;
        let parent_workspaces = (level == DEPTH)
            .then(|| {
                FilesystemProjectWorkspaces::new(
                    &host,
                    aggregate.reducer(),
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &grant_scope,
                    project.clone(),
                )
            })
            .transpose()?;
        let history = ResourceRevision::History(StreamRef::new(
            stream_provider.clone(),
            authority.stream_path(&mut None)?.into_bytes(),
            Some(aggregate.reducer().revision().to_string()),
        )?);
        let request = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes(identity(20 + level)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_result_volume: None,
                summary: None,
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 4,
                maximum_inherited_bytes: 16 * 1_024,
                maximum_inherited_references: 8,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: private.clone(),
                        generation: host
                            .resolve(&workspace_ref(provider.clone(), &private.storage_name()?)?)
                            .await?
                            .generation,
                        paths: Vec::new(),
                    },
                },
                ForkSelection {
                    required: true,
                    revision: history.clone(),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: project.clone(),
                        generation: project_head.generation.clone(),
                    },
                },
            ],
            boundary: None,
        };
        let mut sibling_request = request.clone();
        let report = aggregate.prepare_fork(&preparer, request).await?;
        assert!(
            report
                .reference_grants
                .iter()
                .any(|grant| grant.reader == child_agent && grant.file == model_head)
        );
        let child_attachment_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == previous_attachment)
            .map(|grant| grant.capability())
            .transpose()?
            .ok_or_else(|| Error::Invalid("recursive child attachment grant is missing".into()))?;
        let inherited_file = report
            .inherited_context
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("recursive inherited context is missing".into()))?;
        let mut child_aggregate = open_aggregate(
            &stream,
            child_authority.clone(),
            &child_issuer,
            host.clone(),
            child_scope.clone(),
            stream_provider.clone(),
        )
        .await?;
        let seed = child_aggregate
            .spawn_from_report(
                &mut aggregate,
                report,
                grant_scope.clone(),
                child_scope.clone(),
            )
            .await?;
        assert_eq!(seed.child, child_authority);
        if level == 1 {
            drop(preparer);
            drop(aggregate);
            drop(child_aggregate);
            drop(stream);
            drop(host);
            host = FilesystemHost::new(
                Fs::local(fs_options.clone())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
                &provider, None,
            )?;
            stream = Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            );
            aggregate = open_aggregate(
                &stream,
                authority.clone(),
                &issuer,
                host.clone(),
                grant_scope.clone(),
                stream_provider.clone(),
            )
            .await?;
            child_aggregate = open_aggregate(
                &stream,
                child_authority.clone(),
                &child_issuer,
                host.clone(),
                child_scope.clone(),
                stream_provider.clone(),
            )
            .await?;
            assert!(aggregate.reducer().fork(&child_authority).is_some());
            assert_eq!(child_aggregate.reducer().revision(), 1);
            let sibling_agent = AgentId::from_bytes([99; 16]);
            let sibling_authority = Authority {
                kind: AggregateKind::Conversation,
                id: "local-sibling".into(),
            };
            let sibling_issuer =
                AuthorityIssuer::new("local-recursive-e2e", [7; 32], sibling_authority.clone());
            let sibling_private = volume(
                &provider,
                VolumeClass::AgentPrivate,
                "sibling-private".into(),
                VolumeOwner::Agent(sibling_agent),
            )?;
            let sibling_project = volume(
                &provider,
                VolumeClass::Project,
                "sibling-project".into(),
                VolumeOwner::Project("local-project".into()),
            )?;
            let sibling_scope = scope(
                &sibling_issuer,
                sibling_agent,
                &sibling_private,
                &sibling_project,
            )?;
            sibling_request.operation_id = OperationId::from_bytes(identity(111));
            sibling_request.parent_revision = aggregate.reducer().revision();
            sibling_request.child = sibling_authority.clone();
            sibling_request.child_agent = sibling_agent;
            sibling_request.preparation.child_private_volume = sibling_private.clone();
            sibling_request.preparation.child_project_volume = sibling_project.clone();
            sibling_request
                .selections
                .iter_mut()
                .find(|selection| matches!(selection.revision, ResourceRevision::History(_)))
                .ok_or_else(|| Error::Invalid("missing history selection".into()))?
                .revision = ResourceRevision::History(StreamRef::new(
                stream_provider.clone(),
                authority.stream_path(&mut None)?.into_bytes(),
                Some(aggregate.reducer().revision().to_string()),
            )?);
            let reader = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &grant_scope, 64 * 1024, None)?;
            let sibling_preparer = FilesystemForkPreparer::new(
                host.clone(),
                aggregate.reducer().clone(),
                issuer.verifier(&mut None).expect("verifier copy"),
                grant_scope.clone(),
                project.clone(),
                stream_provider.clone(),
                reader,
                None, None, #[cfg(not(target_arch = "wasm32"))] None,
            )?;
            let report = aggregate
                .prepare_fork(&sibling_preparer, sibling_request)
                .await?;
            assert!(
                report
                    .reference_grants
                    .iter()
                    .any(|grant| grant.reader == sibling_agent && grant.file == model_head)
            );
            let mut sibling = open_aggregate(
                &stream,
                sibling_authority,
                &sibling_issuer,
                host.clone(),
                sibling_scope.clone(),
                stream_provider.clone(),
            )
            .await?;
            sibling
                .spawn_from_report(
                    &mut aggregate,
                    report,
                    grant_scope.clone(),
                    sibling_scope.clone(),
                )
                .await?;
            let changed = host
                .put_content(
                    &root_private,
                    &root_write,
                    "scratch/current.txt",
                    b"later parent scratch",
                    "text/plain",
                    "scratch.txt",
                    1024,
                    &IdempotencyKey::new("later-parent-scratch")?,
                 None, None)
                .await?;
            assert_ne!(Some(&changed), approved_model_files.get(1));
            let (_, sibling_model, _) = model_fork_prefix(
                host.clone(),
                &stream,
                &sibling,
                &sibling_issuer,
                &sibling_scope,
                &sibling_private,
                &sibling_project,
                Some((&model_head, &model_request)),
                &approved_model_files,
                &root_attachment,
                9,
            )
            .await?;
            assert!(
                sibling_model
                    .request()
                    .messages
                    .starts_with(&model_request.request().messages)
            );
            assert_eq!(
                model_head,
                ParentProjectController::new(
                    &host,
                    aggregate.reducer(),
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &grant_scope,
                    &project
                , None)?
                .stage_model_prefix(&root_private, &ModelPrefix::select(&model_request, None)?)
                .await?
            );
        }
        let child_write = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
        )?;
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                &format!("messages/child-{level}.txt"),
                format!("child message {level}").as_bytes(),
                "text/plain",
                &format!("child-{level}.txt"),
                1_024,
                &IdempotencyKey::new(format!("child-message-{level}"))?,
             None, None)
            .await?;
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                &format!("attachments/child-{level}.bin"),
                format!("child attachment {level}").as_bytes(),
                "application/octet-stream",
                &format!("child-{level}.bin"),
                1_024,
                &IdempotencyKey::new(format!("child-attachment-{level}"))?,
             None, None)
            .await?;
        let child_read = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
        )?;
        assert!(
            !host
                .read_content(&inherited_file, &child_read, 16 * 1_024, None)
                .await?
                .is_empty()
        );
        assert!(
            host.read_content(&root_file, &child_read, 1_024, None)
                .await
                .is_err()
        );
        assert_eq!(
            host.read_content(&child_attachment, &child_read, 1_024, None)
                .await?
                .as_ref(),
            format!("child attachment {level}").as_bytes(),
        );
        let denied = FilesystemContentVerifier::new(host.clone(), &child_issuer.verifier(&mut None).expect("verifier copy"), &child_scope, 64 * 1024, None)?;
        assert!(
            PreparedModelRequest::inherit(
                ModelRequest {
                    model: model_request.request().model.clone(),
                    tools: model_request.request().tools.clone(),
                    max_output_tokens: Some(4096),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("denied".into())
                    }],
                },
                &model_head,
                &denied,
                Limits::default(),
                None,
            )
            .await
            .is_err()
        );
        let (child_model_head, child_model_request, child_scratch) = model_fork_prefix(
            host.clone(),
            &stream,
            &child_aggregate,
            &child_issuer,
            &child_scope,
            &child_private,
            &child_project,
            Some((&model_head, &model_request)),
            &approved_model_files,
            &child_attachment,
            level,
        )
        .await?;
        model_head = child_model_head;
        model_request = child_model_request;
        approved_model_files.extend([model_head.clone(), child_scratch, child_attachment.clone()]);
        child_aggregate
            .execute(command(
                40 + level,
                1,
                &child_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([40 + level; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: child_file.clone(),
                        attachments: vec![Attachment {
                            file: child_attachment.clone(),
                            label: None,
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: [("example.model-prefix".into(), model_head.clone())].into(),
                    }),
                },
            )?, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let child_observation = host
            .resolve(&workspace_ref(
                provider.clone(),
                &child_project.storage_name()?,
            )?)
            .await?;
        let child_generation = host
            .apply(
                &child_observation.workspace,
                Some(&child_observation.generation),
                &[WorkspaceMutation::PutFile {
                    path: format!("/level-{level}.txt"),
                    bytes: format!("project child {level}").into_bytes(),
                }],
                &IdempotencyKey::new(format!("project-write-{level}"))?,
            )
            .await?;

        if level == DEPTH {
            merge_parent_authority = Some(authority.clone());
            merge_parent_issuer = Some(issuer.clone());
            merge_parent_scope = Some(grant_scope.clone());
            merge_parent_project = Some(project.clone());
            merge_child_authority = Some(child_authority.clone());
            let parent_workspaces =
                parent_workspaces.ok_or_else(|| Error::Invalid("missing merge binding".into()))?;
            let notice = host
                .put_content(
                    &project,
                    &ContentGrant::verify(
                        &issuer.verifier(&mut None).expect("verifier copy"),
                        &grant_scope,
                        &project,
                        VolumeOperation::Write,
                    )?,
                    "notices/final-merge.txt",
                    b"final project-only merge",
                    "text/plain",
                    "final-merge.txt",
                    1_024,
                    &IdempotencyKey::new("final-merge-notice")?,
                 None, None)
                .await?;
            let merge_message = ConversationMessage {
                id: Uuid::from_bytes([90; 16]),
                sequence: 2,
                kind: MessageKind::Merge,
                content: notice,
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: None,
                tool_call_id: None,
                extensions: Default::default(),
            };
            let plan = parent_workspaces
                .prepare_project_import(
                    &grant_scope,
                    aggregate.reducer(),
                    &child_authority,
                    &child_project,
                    &host
                        .resolve(&workspace_ref(
                            provider.clone(),
                            &(child_project).storage_name()?,
                        )?)
                        .await?
                        .generation,
                )
                .await?;
            let (ProjectJoinOutcome::Applied(receipt)
            | ProjectJoinOutcome::AlreadyApplied(receipt)) = plan
                .apply(
                    &grant_scope,
                    OperationId::from_bytes([91; 16]),
                    &child_authority,
                    &merge_message,
                    &[],
                )
                .await?
            else {
                return Err(Error::Conflict(
                    "final local project merge was not applied".into(),
                ));
            };
            aggregate
                .execute(Command {
                    operation_id: OperationId::from_bytes([91; 16]),
                    idempotency_key: IdempotencyKey::new("publish-final-merge")?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: grant_scope.clone(),
                    causal_parent: None,
                    action: Action::PublishProjectMerge {
                        receipt: Box::new(receipt),
                    },
                }, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
            assert_eq!(
                host.read(
                    &project_head.workspace,
                    None,
                    &format!("/level-{level}.txt"),
                    1_024
                )
                .await?
                .as_ref(),
                format!("project child {level}").as_bytes(),
            );
        }

        final_child_issuer = Some(child_issuer.clone());
        final_child_scope = Some(child_scope.clone());
        final_child_private = Some(child_private.clone());
        private = child_private.clone();
        final_child_file = Some(child_file);
        previous_attachment = child_attachment.clone();
        final_child_attachment = Some(child_attachment);
        project = child_project;
        authority = child_authority;
        issuer = child_issuer.clone();
        grant_scope = child_scope;
        project_head = acyclic_harness::filesystem::WorkspaceObservation {
            workspace: child_observation.workspace,
            generation: child_generation,
        };
        aggregate = child_aggregate;

        if level == 1 {
            let reference_issuer = child_issuer.clone();
            let child_scope_with_reference = reference_issuer.issue("child-root-reference-reader", &Capabilities::new([
                    child_private.capability(VolumeOperation::Read)?,
                    child_attachment_capability,
                ]), Some(child_agent), None, &mut None).expect("scope proof");
            let exact = ContentGrant::verify_read(
                &reference_issuer.verifier(&mut None).expect("verifier copy"),
                &child_scope_with_reference,
                &root_attachment,
            )?;
            assert_eq!(
                host.read_content(&root_attachment, &exact, 1_024, None)
                    .await?
                    .as_ref(),
                b"root attachment"
            );
            assert!(
                ContentGrant::verify_read(
                    &reference_issuer.verifier(&mut None).expect("verifier copy"),
                    &child_scope_with_reference,
                    &root_file,
                )
                .is_err()
            );
        }
    }

    let final_agent = final_child_scope
        .as_ref()
        .and_then(|scope| scope.agent())
        .ok_or_else(|| Error::Invalid("missing final child agent".into()))?;
    let final_issuer =
        final_child_issuer.ok_or_else(|| Error::Invalid("missing final child issuer".into()))?;
    let final_scope =
        final_child_scope.ok_or_else(|| Error::Invalid("missing final child scope".into()))?;
    let final_private = final_child_private
        .ok_or_else(|| Error::Invalid("missing final child private volume".into()))?;
    let final_file =
        final_child_file.ok_or_else(|| Error::Invalid("missing final child file".into()))?;
    let final_attachment = final_child_attachment
        .ok_or_else(|| Error::Invalid("missing final child attachment".into()))?;
    let final_read = ContentGrant::verify(
        &final_issuer.verifier(&mut None).expect("verifier copy"),
        &final_scope,
        &final_private,
        VolumeOperation::Read,
    )?;
    assert_eq!(
        host.read_content(&final_file, &final_read, 1_024, None)
            .await?
            .as_ref(),
        b"child message 3",
    );
    assert!(
        host.read_content(&root_file, &final_read, 1_024, None)
            .await
            .is_err()
    );
    assert_eq!(
        host.read_content(&final_attachment, &final_read, 1_024, None)
            .await?
            .as_ref(),
        b"child attachment 3",
    );
    let delegated = root_issuer.delegate_private_file_read(
        &root_scope,
        final_agent,
        "final-exact-root-attachment",
        &root_attachment,
    )?;
    let exact_read =
        ContentGrant::verify_read(&root_issuer.verifier(&mut None).expect("verifier copy"), &delegated, &root_attachment)?;
    assert_eq!(
        host.read_content(&root_attachment, &exact_read, 1_024, None)
            .await?
            .as_ref(),
        b"root attachment"
    );
    assert!(ContentGrant::verify_read(&root_issuer.verifier(&mut None).expect("verifier copy"), &delegated, &root_file).is_err());

    let merge_parent_authority = merge_parent_authority
        .ok_or_else(|| Error::Invalid("missing merge parent authority".into()))?;
    let merge_parent_issuer =
        merge_parent_issuer.ok_or_else(|| Error::Invalid("missing merge parent issuer".into()))?;
    let merge_parent_scope =
        merge_parent_scope.ok_or_else(|| Error::Invalid("missing merge parent scope".into()))?;
    let merge_parent_project = merge_parent_project
        .ok_or_else(|| Error::Invalid("missing merge parent project".into()))?;
    let merge_child_authority = merge_child_authority
        .ok_or_else(|| Error::Invalid("missing merge child authority".into()))?;
    let reopened_parent = open_aggregate(
        &stream,
        merge_parent_authority,
        &merge_parent_issuer,
        host.clone(),
        merge_parent_scope,
        stream_provider,
    )
    .await?;
    assert_eq!(reopened_parent.reducer().revision(), 4);
    assert!(
        reopened_parent
            .reducer()
            .fork(&merge_child_authority)
            .is_some()
    );
    let merged_project = host
        .resolve(&workspace_ref(
            provider,
            &merge_parent_project.storage_name()?,
        )?)
        .await?;
    assert_eq!(
        host.read(&merged_project.workspace, None, "/level-3.txt", 1_024)
            .await?
            .as_ref(),
        b"project child 3",
    );
    assert!(
        host.read(
            &merged_project.workspace,
            None,
            "/messages/child-3.txt",
            1_024
        )
        .await
        .is_err(),
        "private child conversation files stay outside the project merge"
    );
    Ok(())
}

/// Keep this chain deliberately small enough for regular CI while exercising the same-path allocation and reference rules far beyond a shallow fork.
#[tokio::test]
async fn local_deep_same_path_recursive_forks_keep_parent_controls() -> Result<()> {
    const DEPTH: u8 = 64;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("local-deep-e2e".into(), "filesystem".into(), "2".into(), &mut None)?;
    let stream_provider = ProviderRef::new("local-deep-e2e".into(), "stream".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(
        Fs::local(fs_options)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        &provider, None,
    )?;
    let stream = Arc::new(
        LocalStream::open(&stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );

    let mut authority = Authority {
        kind: AggregateKind::Conversation,
        id: "deep-root".into(),
    };
    let mut agent = AgentId::from_bytes([0; 16]);
    let mut issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], authority.clone());
    let mut private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(agent),
    )?;
    let mut project = volume(
        &provider,
        VolumeClass::Project,
        "deep-project-0".into(),
        VolumeOwner::Project("deep-project".into()),
    )?;
    let mut grant_scope = scope(&issuer, agent, &private, &project)?;
    let mut project_head = host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let mut aggregate = open_aggregate(
        &stream,
        authority.clone(),
        &issuer,
        host.clone(),
        grant_scope.clone(),
        stream_provider.clone(),
    )
    .await?;
    aggregate
        .execute(command(
            1,
            0,
            &grant_scope,
            Action::BindConversation { agent },
        )?, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &private,
        VolumeOperation::Write,
    )?;
    let mut previous_file = host
        .put_content(
            &private,
            &root_write,
            "messages/current.txt",
            b"deep level 0",
            "text/plain",
            "current.txt",
            1_024,
            &IdempotencyKey::new("deep-message-0")?,
         None, None)
        .await?;
    let mut previous_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/evidence.bin",
            b"deep attachment 0",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("deep-attachment-0")?,
         None, None)
        .await?;
    aggregate
        .execute(command(
            2,
            1,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([2; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: previous_file.clone(),
                    attachments: vec![Attachment {
                        file: previous_attachment.clone(),
                        label: None,
                    }]
                    .into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        )?, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;

    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes([level; 16]);
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("deep-child-{level}"),
        };
        let child_issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], child_authority.clone());
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            "scratch".into(),
            VolumeOwner::Agent(child_agent),
        )?;
        assert_ne!(private.storage_name()?, child_private.storage_name()?);
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("deep-project-{level}"),
            VolumeOwner::Project("deep-project".into()),
        )?;
        let child_scope = scope(&child_issuer, child_agent, &child_private, &child_project)?;
        let reader = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &grant_scope, 8 * 1_024, None)?;
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            aggregate.reducer().clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            grant_scope.clone(),
            project.clone(),
            stream_provider.clone(),
            reader,
            None, None, #[cfg(not(target_arch = "wasm32"))] None,
        )?;
        let request = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes(identity(20 + level)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_result_volume: None,
                summary: None,
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 2,
                maximum_inherited_bytes: 8 * 1_024,
                maximum_inherited_references: 4,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: private.clone(),
                        generation: host
                            .resolve(&workspace_ref(provider.clone(), &private.storage_name()?)?)
                            .await?
                            .generation,
                        paths: Vec::new(),
                    },
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        stream_provider.clone(),
                        authority.stream_path(&mut None)?.into_bytes(),
                        Some(aggregate.reducer().revision().to_string()),
                    )?),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: project.clone(),
                        generation: project_head.generation.clone(),
                    },
                },
            ],
            boundary: None,
        };
        let report = aggregate.prepare_fork(&preparer, request).await?;
        let reference_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == previous_attachment)
            .map(|grant| grant.capability())
            .transpose()?
            .ok_or_else(|| Error::Invalid("deep child attachment grant is missing".into()))?;
        let inherited_file = report
            .inherited_context
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("deep inherited context is missing".into()))?;
        let mut child_aggregate = open_aggregate(
            &stream,
            child_authority.clone(),
            &child_issuer,
            host.clone(),
            child_scope.clone(),
            stream_provider.clone(),
        )
        .await?;
        child_aggregate
            .spawn_from_report(
                &mut aggregate,
                report,
                grant_scope.clone(),
                child_scope.clone(),
            )
            .await?;

        let delegated = issuer.delegate_private_file_read(
            &grant_scope,
            child_agent,
            format!("deep-parent-attachment-{level}"),
            &previous_attachment,
        )?;
        let exact_parent_read =
            ContentGrant::verify_read(&issuer.verifier(&mut None).expect("verifier copy"), &delegated, &previous_attachment)?;
        assert_eq!(
            host.read_content(&previous_attachment, &exact_parent_read, 1_024, None)
                .await?
                .as_ref(),
            format!("deep attachment {}", level - 1).as_bytes(),
        );
        assert!(ContentGrant::verify_read(&issuer.verifier(&mut None).expect("verifier copy"), &delegated, &previous_file).is_err());

        let child_read = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
        )?;
        assert!(
            !host
                .read_content(&inherited_file, &child_read, 8 * 1_024, None)
                .await?
                .is_empty()
        );
        assert!(
            host.read_content(&previous_file, &child_read, 1_024, None)
                .await
                .is_err()
        );
        assert!(
            host.read_content(&previous_attachment, &child_read, 1_024, None)
                .await
                .is_err()
        );
        let reference_scope = child_issuer.issue(&format!("deep-reference-reader-{level}"), &Capabilities::new([
                child_private.capability(VolumeOperation::Read)?,
                reference_capability,
            ]), Some(child_agent), None, &mut None).expect("scope proof");
        let child_reference_read = ContentGrant::verify_read(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &reference_scope,
            &previous_attachment,
        )?;
        assert_eq!(
            host.read_content(&previous_attachment, &child_reference_read, 1_024, None)
                .await?
                .as_ref(),
            format!("deep attachment {}", level - 1).as_bytes(),
        );
        assert!(
            ContentGrant::verify_read(&child_issuer.verifier(&mut None).expect("verifier copy"), &reference_scope, &previous_file)
                .is_err()
        );

        let child_write = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
        )?;
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                "messages/current.txt",
                format!("deep level {level}").as_bytes(),
                "text/plain",
                "current.txt",
                1_024,
                &IdempotencyKey::new(format!("deep-message-{level}"))?,
             None, None)
            .await?;
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                "attachments/evidence.bin",
                format!("deep attachment {level}").as_bytes(),
                "application/octet-stream",
                "evidence.bin",
                1_024,
                &IdempotencyKey::new(format!("deep-attachment-{level}"))?,
             None, None)
            .await?;
        child_aggregate
            .execute(command(
                100 + level,
                1,
                &child_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([100 + level; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: child_file.clone(),
                        attachments: vec![Attachment {
                            file: child_attachment.clone(),
                            label: None,
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            )?, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        project_head = host
            .resolve(&workspace_ref(
                provider.clone(),
                &child_project.storage_name()?,
            )?)
            .await?;
        previous_file = child_file;
        previous_attachment = child_attachment;
        private = child_private;
        project = child_project;
        agent = child_agent;
        authority = child_authority;
        issuer = child_issuer;
        grant_scope = child_scope;
        aggregate = child_aggregate;
    }

    assert_eq!(aggregate.reducer().revision(), 2);
    assert_eq!(agent, AgentId::from_bytes([DEPTH; 16]));
    assert_eq!(project.id(), "deep-project-64");
    Ok(())
}
