//! Ref-only durable executor observations across Stream and Filesystem.

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, InteractionId, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::{
        ContentGrant, FileDescriptor, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    durable_tool::DurableToolRunner,
    executor::{
        ExecutionEvent, ExecutionJournal, Executor, StockExecutor, ToolFailureKind, TurnInput,
    },
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    model::{
        FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
        ModelProvider, ModelRequest,
    },
    resources::ProviderRef,
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_harness_filesystem::{FilesystemExecutionJournal, FilesystemHost};
use acyclic_stream::{MemoryStream, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct TextModel(AtomicUsize);

async fn bind_conversation(
    stream: &StreamClient<MemoryStream>,
    issuer: &AuthorityIssuer,
    id: &str,
    agent: AgentId,
) -> Result<()> {
    let mut aggregate = StreamAggregate::open(
        stream,
        Authority {
            kind: AggregateKind::Conversation,
            id: id.into(),
        },
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    aggregate
        .execute(Command {
            operation_id: OperationId::from_bytes([30; 16]),
            idempotency_key: IdempotencyKey::new(format!("bind:{id}"))?,
            expected_revision: 0,
            scope: issuer.root("owner", Capabilities::new(["conversation:bind"])),
            causal_parent: None,
            action: Action::BindConversation { agent },
        })
        .await?;
    Ok(())
}

struct NoopTool;
impl ToolExecutor for NoopTool {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async { Ok(ToolResult { value: Value::Null }) })
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
impl ToolProjection for NoopTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

#[derive(Default)]
struct CapturingModel(Mutex<Vec<ModelRequest>>);

impl ModelProvider for CapturingModel {
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        Box::pin(stream::iter(vec![Ok(ModelEvent::Completed {
            metadata: Value::Null,
        })]))
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

impl ModelProvider for TextModel {
    fn generate<'a>(&'a self, _: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::Content {
                delta: "private answer".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn two_hosts_cannot_both_claim_one_tool_dispatch() -> Result<()> {
    let provider = ProviderRef::new("tool-claim-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([55; 16]);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "tool-claim-e2e",
        [55; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "tool-claim-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        agent,
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
        ]),
    );
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let journal_a = FilesystemExecutionJournal::new(
        stream.clone(),
        host.clone(),
        private.clone(),
        issuer.verifier(),
        scope.clone(),
        4_096,
    )?;
    let journal_b =
        FilesystemExecutionJournal::new(stream, host, private, issuer.verifier(), scope, 4_096)?;
    let operation = OperationId::from_bytes([56; 16]);
    journal_a
        .append(
            operation,
            "tool:started".into(),
            ExecutionEvent::Started {
                request_digest: [57; 32],
            },
        )
        .await?;
    let invocation = journal_a
        .stage(
            operation,
            "tool:invocation".into(),
            b"{}".to_vec(),
            "application/json",
        )
        .await?;
    let event = ExecutionEvent::ToolStarted {
        step: 0,
        call_id: operation.to_string(),
        invocation,
    };
    let (first, second) = tokio::join!(
        journal_a.append_if_tail(operation, 1, "claim-a".into(), event.clone()),
        journal_b.append_if_tail(operation, 1, "claim-b".into(), event),
    );
    assert_ne!(first?, second?);
    assert_eq!(journal_a.replay(operation).await?.len(), 2);
    let (first_terminal, second_terminal) = tokio::join!(
        journal_a.append_if_tail(
            operation,
            2,
            "terminal-a".into(),
            ExecutionEvent::ToolFailed {
                step: 0,
                call_id: operation.to_string(),
                reason: ToolFailureKind::ExecutorRejected
            }
        ),
        journal_b.append_if_tail(
            operation,
            2,
            "terminal-b".into(),
            ExecutionEvent::ToolFailed {
                step: 0,
                call_id: operation.to_string(),
                reason: ToolFailureKind::InvalidOutput
            }
        ),
    );
    assert_ne!(first_terminal?, second_terminal?);
    assert_eq!(journal_a.replay(operation).await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn durable_tool_replay_is_bound_to_its_admitting_task() -> Result<()> {
    let provider = ProviderRef::new("tool-owner-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([58; 16]);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "tool-owner-e2e",
        [58; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "tool-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        agent,
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
        ]),
    );
    let journal = Arc::new(FilesystemExecutionJournal::new(
        StreamClient::new(Arc::new(MemoryStream::default())),
        host,
        private,
        issuer.verifier(),
        scope,
        4_096,
    )?);
    let definition = ToolDefinition {
        name: "example.noop".into(),
        revision: "1".into(),
        description: "No-op".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
    };
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: definition.clone(),
        executor: Arc::new(NoopTool),
        projection: Arc::new(NoopTool),
    })?;
    let invalid_output = ToolDefinition {
        name: "example.invalid-output".into(),
        revision: "1".into(),
        description: "Pinned invalid result".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"string"}),
    };
    registry.register(Tool {
        definition: invalid_output.clone(),
        executor: Arc::new(NoopTool),
        projection: Arc::new(NoopTool),
    })?;
    let runner = DurableToolRunner::new(registry, journal.clone());
    let operation = OperationId::from_bytes([59; 16]);
    let first_task = TaskId::from_bytes([60; 16]);
    let other_task = TaskId::from_bytes([61; 16]);
    assert!(matches!(
        runner
            .run(first_task, operation, definition.clone(), json!({}))
            .await?,
        Outcome::Succeeded(Value::Null)
    ));
    assert!(matches!(
        runner
            .run(first_task, operation, definition.clone(), json!({}))
            .await?,
        Outcome::Succeeded(Value::Null)
    ));
    assert!(
        runner
            .run(other_task, operation, definition, json!({}))
            .await
            .is_err()
    );
    let failure_operation = OperationId::from_bytes([62; 16]);
    let first_failure = runner
        .run(
            first_task,
            failure_operation,
            invalid_output.clone(),
            json!({}),
        )
        .await?;
    assert!(matches!(&first_failure, Outcome::Failed { .. }));
    assert_eq!(
        first_failure,
        runner
            .run(first_task, failure_operation, invalid_output, json!({}))
            .await?
    );
    assert!(matches!(
        journal
            .replay(failure_operation)
            .await?
            .last()
            .map(|record| &record.event),
        Some(ExecutionEvent::ToolFailed { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn stream_journal_keeps_model_body_in_pinned_private_files() -> Result<()> {
    let provider = ProviderRef::new("journal-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([42; 16]);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "journal-e2e",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "journal-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        agent,
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
        ]),
    );
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host,
        private,
        issuer.verifier(),
        scope,
        4_096,
    )?;
    let model = Arc::new(TextModel(AtomicUsize::new(0)));
    let executor = StockExecutor::new(
        Model::new("test", "text", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let operation_id = OperationId::from_bytes([9; 16]);
    let input = TurnInput {
        operation_id,
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 2,
    };
    let first = executor.execute(input.clone(), &journal).await?;
    let again = executor.execute(input, &journal).await?;
    assert_eq!(first, again);
    assert_eq!(first.text, "private answer");
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    let records = journal.replay(operation_id).await?;
    assert_eq!(records.len(), 4);
    let stream = stream
        .stream(format!("harness/v2/execution/{operation_id}"))
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    use futures::TryStreamExt as _;
    let raw = stream
        .read(0, 16)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert!(raw.iter().all(|record| {
        !record
            .value
            .windows(b"private answer".len())
            .any(|window| window == b"private answer")
    }));
    Ok(())
}

#[tokio::test]
async fn interactions_are_ref_only_authenticated_and_single_resolution() -> Result<()> {
    let provider = ProviderRef::new("interaction-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([43; 16])),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "interaction-e2e",
        [8; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "interaction-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([43; 16]),
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            "interaction:open".to_owned(),
        ]),
    );
    let id = InteractionId::from_bytes([11; 16]);
    let responder = issuer.root(
        "human",
        Capabilities::new([
            "interaction:resolve".to_owned(),
            format!("interaction:respond:{id}"),
        ]),
    );
    let unauthorized = issuer.root("bystander", Capabilities::default());
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    bind_conversation(
        &stream,
        &issuer,
        "interaction-owner",
        AgentId::from_bytes([43; 16]),
    )
    .await?;
    let journal = FilesystemExecutionJournal::new(
        stream.clone(),
        host,
        private,
        issuer.verifier(),
        scope,
        4_096,
    )?;
    let request = Interaction::question("The private prompt", json!({"type":"string"}))?;
    journal.open_interaction(id, request.clone()).await?;
    journal.open_interaction(id, request).await?;
    assert!(journal.interaction_outcome(id).await?.is_none());
    let response = InteractionResponse::Question {
        value: json!("private answer"),
    };
    assert!(
        journal
            .resolve_interaction(id, response.clone(), &unauthorized)
            .await
            .is_err()
    );
    assert!(
        journal
            .resolve_interaction(
                id,
                InteractionResponse::Question { value: json!(17) },
                &responder
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(id, response.clone(), &responder)
        .await?;
    assert!(matches!(
        journal.interaction_outcome(id).await?,
        Some(InteractionOutcome::Answered { .. })
    ));
    journal
        .resolve_interaction(id, response.clone(), &responder)
        .await?;
    assert!(
        journal
            .resolve_interaction(
                id,
                InteractionResponse::Question {
                    value: json!("different")
                },
                &responder
            )
            .await
            .is_err()
    );
    let raw = stream
        .stream("harness/v2/conversations/interaction-owner")
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 3)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    use futures::TryStreamExt as _;
    let records = raw
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(records.len(), 3);
    for record in records {
        assert!(
            !record
                .value
                .windows(b"private prompt".len())
                .any(|window| window == b"private prompt")
        );
        assert!(
            !record
                .value
                .windows(b"private answer".len())
                .any(|window| window == b"private answer")
        );
    }
    Ok(())
}

#[tokio::test]
async fn scoped_tool_install_reads_the_durable_approval_not_a_caller_claim() -> Result<()> {
    let provider = ProviderRef::new("tool-approval-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([46; 16])),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "tool-approval-e2e",
        [10; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "tool-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([46; 16]),
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            "tool:install:example.echo".to_owned(),
            "interaction:open".to_owned(),
        ]),
    );
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    bind_conversation(
        &stream,
        &issuer,
        "tool-owner",
        AgentId::from_bytes([46; 16]),
    )
    .await?;
    let journal = FilesystemExecutionJournal::new(
        stream,
        host,
        private,
        issuer.verifier(),
        scope.clone(),
        4_096,
    )?;
    let definition = ToolDefinition {
        name: "example.echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
    };
    let digest = definition.digest()?;
    let operation = OperationId::from_bytes([14; 16]);
    let denied_id = InteractionId::from_bytes([15; 16]);
    let approved_id = InteractionId::from_bytes([16; 16]);
    let responder = issuer.root(
        "human",
        Capabilities::new([
            "interaction:resolve".to_owned(),
            format!("interaction:respond:{denied_id}"),
            format!("interaction:respond:{approved_id}"),
        ]),
    );
    let approval = Interaction::approval("Install echo", operation, digest)?;
    journal
        .open_interaction(denied_id, approval.clone())
        .await?;
    journal.open_interaction(approved_id, approval).await?;
    let tool = Tool {
        definition,
        executor: Arc::new(NoopTool),
        projection: Arc::new(NoopTool),
    };
    let mut registry = ToolRegistry::new();
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                operation,
                &scope,
                &issuer.verifier(),
                approved_id,
                &journal
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(
            denied_id,
            InteractionResponse::Approval {
                approved: false,
                reason: None,
            },
            &responder,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                operation,
                &scope,
                &issuer.verifier(),
                denied_id,
                &journal
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(
            approved_id,
            InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            &responder,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                OperationId::from_bytes([17; 16]),
                &scope,
                &issuer.verifier(),
                approved_id,
                &journal
            )
            .await
            .is_err()
    );
    registry
        .install_scoped(
            tool,
            operation,
            &scope,
            &issuer.verifier(),
            approved_id,
            &journal,
        )
        .await?;
    assert!(registry.get("example.echo").is_some());
    Ok(())
}

#[tokio::test]
async fn typed_file_input_requires_resident_authorized_bytes_before_journaling() -> Result<()> {
    let provider = ProviderRef::new("file-input-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let private = VolumeRef::new(
        provider.clone(),
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([44; 16])),
    )?;
    let foreign = VolumeRef::new(
        provider.clone(),
        "foreign",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([45; 16])),
    )?;
    let project = VolumeRef::new(
        provider,
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    host.create_volume(&private).await?;
    host.create_volume(&foreign).await?;
    host.create_volume(&project).await?;
    let issuer = AuthorityIssuer::new(
        "file-input-e2e",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "file-input-owner".into(),
        },
    );
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([44; 16]),
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            foreign.capability(VolumeOperation::Read)?,
            foreign.capability(VolumeOperation::Write)?,
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
        ]),
    );
    let grant = ContentGrant::verify(&issuer.verifier(), &scope, &project, VolumeOperation::Write)?;
    let file = host
        .put_content(
            &project,
            &grant,
            "images/chart.png",
            &[1, 2, 3],
            "image/png",
            "chart.png",
            4_096,
            &IdempotencyKey::new("chart-upload")?,
        )
        .await?;
    let foreign_issuer = AuthorityIssuer::new(
        "foreign-owner",
        [45; 32],
        Authority {
            kind: AggregateKind::Agent,
            id: AgentId::from_bytes([45; 16]).to_string(),
        },
    );
    let foreign_scope = foreign_issuer.root_for_agent(
        AgentId::from_bytes([45; 16]),
        "foreign-owner",
        Capabilities::new([foreign.capability(VolumeOperation::Write)?]),
    );
    assert!(
        ContentGrant::verify(&issuer.verifier(), &scope, &foreign, VolumeOperation::Write).is_err()
    );
    let foreign_grant = ContentGrant::verify(
        &foreign_issuer.verifier(),
        &foreign_scope,
        &foreign,
        VolumeOperation::Write,
    )?;
    let foreign_file = host
        .put_content(
            &foreign,
            &foreign_grant,
            "secret.txt",
            b"secret",
            "text/plain",
            "secret.txt",
            4_096,
            &IdempotencyKey::new("foreign-upload")?,
        )
        .await?;
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let journal =
        FilesystemExecutionJournal::new(stream, host, private, issuer.verifier(), scope, 4_096)?;
    let model = Arc::new(CapturingModel::default());
    let executor = StockExecutor::new(
        Model::new("test", "capture", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let content = ModelContent::Parts(vec![
        ModelContentPart::Text {
            text: "describe".into(),
        },
        ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native,
        },
    ]);
    let invalid = FileRef::new(
        project,
        "images/chart.png",
        file.version(),
        FileDescriptor::from_bytes(b"other", "image/png")?,
        "chart.png",
    )?;
    let operation_id = OperationId::from_bytes([12; 16]);
    assert!(
        executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Part(ModelContentPart::File {
                        file: invalid,
                        policy: FileProjectionPolicy::Native
                    }),
                    selected_context: None,
                    max_steps: 1
                },
                &journal
            )
            .await
            .is_err()
    );
    assert!(journal.replay(operation_id).await?.is_empty());
    assert!(
        executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Part(ModelContentPart::File {
                        file: foreign_file,
                        policy: FileProjectionPolicy::BoundedFull
                    }),
                    selected_context: None,
                    max_steps: 1
                },
                &journal
            )
            .await
            .is_err()
    );
    assert!(journal.replay(operation_id).await?.is_empty());
    executor
        .execute(
            TurnInput {
                operation_id,
                input: content.clone(),
                selected_context: None,
                max_steps: 1,
            },
            &journal,
        )
        .await?;
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.len(), 1);
    assert_eq!(requests[0].messages[0].content, content);
    Ok(())
}
