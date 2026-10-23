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
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
    ModelPrefix, ModelRequest, ModelRole, PreparedModelRequest,
};
use acyclic_harness::resources::{ProviderRef, StreamRef};
use acyclic_harness::runtime::RuntimeScope;
use acyclic_harness::store::StreamAggregate;
use acyclic_harness::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};
use acyclic_harness::tool::{
    JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
    ToolResult,
};
use acyclic_harness::{AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

const DEPTH: u8 = 3;

struct Echo;

impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
fn reuse_prefix(warm: Option<&ModelRequest>, request: &ModelRequest) -> Result<bool> {
    let Some(warm) = warm else { return Ok(false) };
    if warm.model.name != request.model.name {
        return Err(Error::Unsupported(
            "fixture cannot reuse another model profile".into(),
        ));
    }
    Ok(warm.model == request.model && request.messages.starts_with(&warm.messages))
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
        65_536,
        &mut None,
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
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let mut capabilities = local_scope
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for file in approved {
        capabilities.push(file.read_capability(&mut None)?);
    }
    let read_scope = issuer
        .issue(
            "prefix-owner-approved-reader",
            &Capabilities::new(capabilities),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let reader = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &read_scope,
        64 * 1024,
        None,
    )?;
    let journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &acyclic_harness::core::SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &private,
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: &read_scope,
            maximum_bytes: 64 * 1024,
            io: None,
        },
    )?
    .with_input_verifier(reader.clone());
    let children = if level == 0 {
        json!({"children": ["local-child-1", "local-sibling"]})
    } else {
        json!({"children": [format!("local-child-{}", level + 1)]})
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                    "fork-plan".into(),
                    "example.fork".into(),
                    children,
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            })]),
        ])
        .without_accounting(),
    );
    let model = Model::new(
        "example".into(),
        if level >= 3 { "alternate" } else { "exact" }.into(),
        if level == 2 { "2" } else { "1" }.into(),
        Value::Null,
    )?;
    let mut tools = ToolRegistry::default();
    tools.register((Tool { hook_names: &[], definition: ToolDefinition { name: "example.fork".into(), revision: "1".into(), description: "Plan child".into(),
        input_schema: json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}),
        output_schema: json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({"type":"object","required":["children"],"properties":{"children":{"type":"array","items":{"type":"string"}}},"additionalProperties":false})) },
        executor: Arc::new(Echo), projection: Arc::new(JsonToolProjection) }, None), &mut None, None)?;
    let mut executor =
        StockExecutor::new(model, provider.clone(), ContextPipeline::default(), tools)
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
        &project,
        None,
    )?;
    let private_workspace = workspace_ref(
        private.provider().clone(),
        &private.storage_name(&mut None)?,
    )?;
    let before = host.resolve(&private_workspace, None).await?;
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
        host.resolve(&private_workspace, None).await?.generation
    );
    executor
        .execute(input.clone(), &journal)
        .await
        .map_err(|error| Error::Storage(format!("prefix execute level {level}: {error}")))?;
    let completed = executor
        .completed_tool_prefix(&journal, &input, 0, Some("fork-plan"))
        .await
        .map_err(|error| Error::Storage(format!("prefix extract level {level}: {error}")))?;
    let captured = provider.requests();
    let dispatches = provider.dispatches();
    let mut expected = completed.request().clone();
    expected.messages =
        previous.map_or_else(Vec::new, |(_, parent)| parent.request().messages.clone());
    expected.messages.push(ModelMessage {
        role: ModelRole::User,
        content: input.input.clone(),
    });
    let expected = PreparedModelRequest::prepare(expected, Limits::default(), None)?;
    assert_eq!(captured.first(), Some(expected.request()));
    assert_eq!(captured.get(1), Some(completed.request()));
    assert_eq!(
        dispatches.first().map(|dispatch| dispatch.request_digest),
        Some(expected.request_digest())
    );
    assert_eq!(
        dispatches.get(1).map(|dispatch| dispatch.request_digest),
        Some(completed.request_digest())
    );
    drop(captured);
    drop(dispatches);
    let cold_journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &private,
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: &read_scope,
            maximum_bytes: 64 * 1024,
            io: None,
        },
    )?
    .with_input_verifier(reader);
    executor.execute(input.clone(), &cold_journal).await?;
    let expected_reuse = match level {
        0 => Some(false),
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };
    let requests = provider.requests();
    let first = requests
        .first()
        .ok_or_else(|| Error::Invalid("missing first request".into()))?;
    let second = requests
        .get(1)
        .ok_or_else(|| Error::Invalid("missing continuation request".into()))?;
    let reuse = reuse_prefix(previous.map(|(_, parent)| parent.request()), first);
    match expected_reuse {
        Some(expected) => assert_eq!(reuse?, expected),
        None => assert!(matches!(reuse, Err(Error::Unsupported(_)))),
    }
    assert!(reuse_prefix(Some(first), second)?);
    assert_eq!(requests.len(), 2);
    assert_eq!(provider.remaining(), 0);
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
    VolumeRef::new(provider.clone(), id, class, owner, &mut None)
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
        &mut None,
    )?;
    let source_reads = [selected.capability(VolumeOperation::Read, &mut None)?];
    Ok(issuer
        .issue(
            &format!("agent-{agent:?}"),
            &Capabilities::new(
                [
                    "conversation:bind".to_owned(),
                    "conversation:append".to_owned(),
                    "fork:publish".to_owned(),
                    "project:merge".to_owned(),
                    project.capability(VolumeOperation::Read, &mut None)?,
                    project.capability(VolumeOperation::Write, &mut None)?,
                    private.capability(VolumeOperation::Read, &mut None)?,
                    private.capability(VolumeOperation::Write, &mut None)?,
                ]
                .into_iter()
                .chain(source_reads),
            ),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof"))
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
    let reader = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        64 * 1_024,
        None,
    )?;
    let forks = CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    Ok(StreamAggregate::open(
        stream,
        (
            authority,
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
    .with_content_verifier(reader)
    .with_fork_verifier(forks)
    .with_merge_verifier(FilesystemProjectMergeVerifier::new(host, None)?))
}

#[path = "../local_recursive_fork/deep.rs"]
mod deep;
#[path = "../local_recursive_fork/recursive.rs"]
mod recursive;
