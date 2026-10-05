//! Ref-only durable executor observations across Stream and Filesystem.
#![cfg(feature = "filesystem")]
#![allow(clippy::too_many_lines, clippy::indexing_slicing)]

use acyclic_fs::Fs;
use acyclic_harness::filesystem::{
    FilesystemExecutionJournal, FilesystemHost, FilesystemInteractionHost,
    InteractionApprovalAuthorization, InteractionOperatorAuthorizer,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, InteractionId, OperationId, Outcome, Result,
    TaskId,
    context::{Context, ContextInput, ContextPipeline, ContextStage},
    conversation::{
        ContentGrant, FileDescriptor, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    durable_tool::DurableToolRunner,
    executor::{
        ExecutionEvent, ExecutionJournal, ExecutionRecord, Executor, StockExecutor,
        ToolFailureKind, TurnInput,
    },
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    model::{
        FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
        ModelProvider, ModelRequest,
    },
    resources::ProviderRef,
    runtime::{Bindings, RuntimeScope, TaskAdmissionRecord, TaskStateProvider, ToolContext},
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
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

struct CountingNoopTool {
    executions: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

impl ToolExecutor for CountingNoopTool {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolResult { value: Value::Null }) })
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
}

/// Models an executor that performed an external effect before losing its
/// result. Recovery must reconcile the durable claim rather than classify the
/// conflict as a terminal tool failure or invoke the effect a second time.
struct EffectThenConflictTool {
    effects: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

impl ToolExecutor for EffectThenConflictTool {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(Error::Conflict(
                "effect completed before reply was lost".into(),
            ))
        })
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Some(ToolResult { value: Value::Null })) })
    }
}

/// Simulates a terminal race: the committed winner is observable, but this
/// caller receives the losing CAS outcome and must replay without redispatch.
struct TerminalCasLoser {
    inner: Arc<dyn ExecutionJournal>,
    losses: AtomicUsize,
}

impl ExecutionJournal for TerminalCasLoser {
    fn replay<'a>(&'a self, operation: OperationId) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
        self.inner.replay(operation)
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.append(operation, key, event)
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            let terminal = matches!(
                &event,
                ExecutionEvent::ToolCompleted { .. } | ExecutionEvent::ToolFailed { .. }
            );
            let committed = self
                .inner
                .append_if_tail(operation, tail, key, event)
                .await?;
            if terminal && committed && self.losses.fetch_add(1, Ordering::SeqCst) < 2 {
                return Ok(false);
            }
            Ok(committed)
        })
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        self.inner.stage(operation, key, bytes, media_type)
    }

    fn load<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        self.inner.load(file)
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.open_interaction(id, interaction)
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        self.inner.interaction_outcome(id)
    }
}

struct TestTaskState(RuntimeScope);
impl TaskStateProvider for TestTaskState {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    fn observe_admission<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async { Err(Error::Unsupported("not used by this journal test".into())) })
    }
    fn resume_scope<'a>(
        &'a self,
        _: TaskId,
        _: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
    fn outcome<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async { Ok(None) })
    }
    fn cancel<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

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
    fn generate<'a>(&'a self, prepared: acyclic_harness::model_input::PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
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
    fn generate<'a>(&'a self, _: acyclic_harness::model_input::PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
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
        model_output_schema: json!({}),
    };
    let executions = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let counted = Arc::new(CountingNoopTool {
        executions: executions.clone(),
        reconciliations: reconciliations.clone(),
    });
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: definition.clone(),
        executor: counted.clone(),
        projection: Arc::new(NoopTool),
    })?;
    let invalid_output = ToolDefinition {
        name: "example.invalid-output".into(),
        revision: "1".into(),
        description: "Pinned invalid result".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"string"}),
        model_output_schema: json!({"type":"string"}),
    };
    registry.register(Tool {
        definition: invalid_output.clone(),
        executor: counted,
        projection: Arc::new(NoopTool),
    })?;
    let racing_journal = Arc::new(TerminalCasLoser {
        inner: journal.clone(),
        losses: AtomicUsize::new(0),
    });
    let runner = DurableToolRunner::new(registry, racing_journal.clone());
    let operation = OperationId::from_bytes([59; 16]);
    let first_task = TaskId::from_bytes([60; 16]);
    let other_task = TaskId::from_bytes([61; 16]);
    let task_scope = RuntimeScope::new(
        Capabilities::new(["tool:call:example.noop", "tool:call:example.invalid-output"]),
        Limits::default(),
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = task_scope.clone();
    bindings.state = Some(Arc::new(TestTaskState(task_scope)));
    let runtime = bindings.build()?;
    let first_context = runtime
        .durable_context(first_task, OperationId::from_bytes([63; 16]))
        .await?;
    let other_context = runtime
        .durable_context(other_task, OperationId::from_bytes([64; 16]))
        .await?;
    assert!(matches!(
        runner
            .run_with_context(
                first_task,
                operation,
                definition.clone(),
                json!({}),
                ToolContext::new(first_context.clone(), operation, operation.to_string())?
            )
            .await?,
        Outcome::Succeeded(Value::Null)
    ));
    assert!(matches!(
        runner
            .run_with_context(
                first_task,
                operation,
                definition.clone(),
                json!({}),
                ToolContext::new(first_context.clone(), operation, operation.to_string())?
            )
            .await?,
        Outcome::Succeeded(Value::Null)
    ));
    assert!(
        runner
            .run_with_context(
                other_task,
                operation,
                definition,
                json!({}),
                ToolContext::new(other_context, operation, operation.to_string())?
            )
            .await
            .is_err()
    );
    let failure_operation = OperationId::from_bytes([62; 16]);
    let first_failure = runner
        .run_with_context(
            first_task,
            failure_operation,
            invalid_output.clone(),
            json!({}),
            ToolContext::new(
                first_context.clone(),
                failure_operation,
                failure_operation.to_string(),
            )?,
        )
        .await?;
    assert!(matches!(&first_failure, Outcome::Failed { .. }));
    assert_eq!(
        first_failure,
        runner
            .run_with_context(
                first_task,
                failure_operation,
                invalid_output,
                json!({}),
                ToolContext::new(
                    first_context,
                    failure_operation,
                    failure_operation.to_string()
                )?
            )
            .await?
    );
    assert_eq!(racing_journal.losses.load(Ordering::SeqCst), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
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
async fn post_claim_conflict_stays_indeterminate_until_reconciliation() -> Result<()> {
    let provider = ProviderRef::new("tool-recovery-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([68; 16]);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        "tool-recovery-e2e",
        [68; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "tool-recovery-owner".into(),
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
        name: "example.effect-then-conflict".into(),
        revision: "1".into(),
        description: "Effect then lost result".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        model_output_schema: json!({}),
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(EffectThenConflictTool {
        effects: effects.clone(),
        reconciliations: reconciliations.clone(),
    });
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: definition.clone(),
        executor,
        projection: Arc::new(NoopTool),
    })?;
    let runner = DurableToolRunner::new(registry, journal.clone());
    let task = TaskId::from_bytes([69; 16]);
    let operation = OperationId::from_bytes([70; 16]);
    let task_scope = RuntimeScope::new(
        Capabilities::new(["tool:call:example.effect-then-conflict"]),
        Limits::default(),
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = task_scope.clone();
    bindings.state = Some(Arc::new(TestTaskState(task_scope)));
    let runtime = bindings.build()?;
    let durable = runtime
        .durable_context(task, OperationId::from_bytes([71; 16]))
        .await?;
    let context = ToolContext::new(durable, operation, operation.to_string())?;

    assert!(matches!(
        runner
            .run_with_context(
                task,
                operation,
                definition.clone(),
                json!({}),
                context.clone(),
            )
            .await?,
        Outcome::Indeterminate { operation_id } if operation_id == operation
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    assert!(!journal
        .replay(operation)
        .await?
        .iter()
        .any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. })));

    assert!(matches!(
        runner
            .run_with_context(task, operation, definition, json!({}), context)
            .await?,
        Outcome::Succeeded(Value::Null)
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
    assert!(matches!(
        journal
            .replay(operation)
            .await?
            .last()
            .map(|record| &record.event),
        Some(ExecutionEvent::ToolCompleted { .. })
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
        host.clone(),
        private.clone(),
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
    assert_eq!(records.len(), 5);
    let (manifest, request) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelInputPrepared {
                manifest, request, ..
            } => Some((manifest, request)),
            _ => None,
        })
        .ok_or_else(|| acyclic_harness::Error::Storage("missing model input admission".into()))?;
    let manifest: acyclic_harness::model_input::ModelInputManifest =
        serde_json::from_slice(&journal.load(manifest).await?).unwrap();
    let request_bytes = journal.load(request).await?;
    assert_eq!(
        *blake3::hash(&request_bytes).as_bytes(),
        manifest.request_digest
    );
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
    let interaction_host = FilesystemInteractionHost::new(
        stream.clone(),
        Arc::clone(&host),
        Authority {
            kind: AggregateKind::Conversation,
            id: "interaction-owner".into(),
        },
        issuer.verifier(),
        SchemaRegistry::new(),
        scope.clone(),
        private.clone(),
        4_096,
    )?;
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
    let first_staged = interaction_host
        .stage_answer(
            id,
            1,
            OperationId::from_bytes([60; 16]),
            &InteractionResponse::Question {
                value: json!("orphaned first attempt"),
            },
        )
        .await?;
    let second_staged = interaction_host
        .stage_answer(
            id,
            1,
            OperationId::from_bytes([61; 16]),
            &InteractionResponse::Question {
                value: json!("competing answer"),
            },
        )
        .await?;
    assert_ne!(first_staged.path(), second_staged.path());
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
        stream.clone(),
        host.clone(),
        private.clone(),
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
        model_output_schema: json!({}),
    };
    let digest = definition.digest()?;
    let denied_operation = OperationId::from_bytes([14; 16]);
    let approved_operation = OperationId::from_bytes([17; 16]);
    let denied_id = InteractionId::from_bytes([15; 16]);
    let approved_id = InteractionId::from_bytes([16; 16]);
    let operator = InteractionOperatorAuthorizer::new(
        Arc::new(FilesystemInteractionHost::new(
            stream.clone(),
            host.clone(),
            Authority {
                kind: AggregateKind::Conversation,
                id: "tool-owner".into(),
            },
            issuer.verifier(),
            SchemaRegistry::new(),
            scope.clone(),
            private.clone(),
            4_096,
        )?),
        issuer.clone(),
    );
    let denied_approval = Interaction::approval("Install echo", denied_operation, digest)?;
    let approved_approval = Interaction::approval("Install echo", approved_operation, digest)?;
    journal
        .open_interaction(denied_id, denied_approval)
        .await?;
    journal
        .open_interaction(approved_id, approved_approval)
        .await?;
    let denied_scope = operator
        .issue_scope(&InteractionApprovalAuthorization {
            interaction_id: denied_id,
            operation_id: denied_operation,
            action_digest: digest,
            approved: false,
        })
        .await?;
    let approved_scope = operator
        .issue_scope(&InteractionApprovalAuthorization {
            interaction_id: approved_id,
            operation_id: approved_operation,
            action_digest: digest,
            approved: true,
        })
        .await?;
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
                approved_operation,
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
            &denied_scope,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                denied_operation,
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
            &approved_scope,
        )
        .await?;
    assert!(
        registry
            .install_scoped(
                tool.clone(),
                OperationId::from_bytes([18; 16]),
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
            approved_operation,
            &scope,
            &issuer.verifier(),
            approved_id,
            &journal,
        )
        .await?;
    assert!(registry.get("example.echo").is_some());
    Ok(())
}

struct InjectFileContext(FileRef);

impl ContextStage for InjectFileContext {
    fn name(&self) -> &str {
        "test.inject-file"
    }

    fn contract(&self) -> Value {
        json!({ "file": self.0 })
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            context.messages.push(acyclic_harness::model::ModelMessage {
                role: acyclic_harness::model::ModelRole::User,
                content: ModelContent::Part(ModelContentPart::File {
                    file: self.0.clone(),
                    policy: FileProjectionPolicy::BoundedFull,
                }),
            });
            Ok(context)
        })
    }
}

struct InjectUnpairedResult;

impl ContextStage for InjectUnpairedResult {
    fn name(&self) -> &str {
        "test.inject-unpaired-result"
    }

    fn contract(&self) -> Value {
        json!({"revision": 1})
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            context.messages.push(acyclic_harness::model::ModelMessage {
                role: acyclic_harness::model::ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "orphan".into(),
                    name: "test.tool".into(),
                    value: Value::Null,
                }),
            });
            Ok(context)
        })
    }
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
    // A ref can be resident in the same private volume and still belong to a
    // different execution. Reusing it must fail before the event is appended.
    let copied_from_operation = OperationId::from_bytes([11; 16]);
    let copied_invocation = journal
        .stage(
            copied_from_operation,
            "copied-invocation".into(),
            b"{}".to_vec(),
            "application/json",
        )
        .await?;
    let content = ModelContent::Parts(vec![
        ModelContentPart::Text {
            text: "describe".into(),
        },
        ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::Native,
        },
    ]);
    let operation_id = OperationId::from_bytes([12; 16]);
    assert!(matches!(
        journal
            .append(
                operation_id,
                "cross-operation-ref".into(),
                ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: "copied-call".into(),
                    invocation: copied_invocation,
                },
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    let invalid = FileRef::new(
        project,
        "images/chart.png",
        file.version(),
        FileDescriptor::from_bytes(b"other", "image/png")?,
        "chart.png",
    )?;
    let injected_operation = OperationId::from_bytes([13; 16]);
    let injected_executor = StockExecutor::new(
        Model::new("test", "capture", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::default().with(Arc::new(InjectFileContext(foreign_file.clone()))),
        ToolRegistry::default(),
    );
    assert!(
        injected_executor
            .execute(
                TurnInput {
                    operation_id: injected_operation,
                    input: ModelContent::Text("ordinary input".into()),
                    selected_context: None,
                    max_steps: 1,
                },
                &journal,
            )
            .await
            .is_err()
    );
    assert!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        journal
            .replay(injected_operation)
            .await?
            .iter()
            .all(|record| !matches!(
                record.event,
                ExecutionEvent::ModelInputPrepared { .. }
                    | ExecutionEvent::ModelStarted { .. }
                    | ExecutionEvent::Model { .. }
            ))
    );
    let orphan_operation = OperationId::from_bytes([14; 16]);
    let orphan_executor = StockExecutor::new(
        Model::new("test", "capture", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::default().with(Arc::new(InjectUnpairedResult)),
        ToolRegistry::default(),
    );
    assert!(matches!(
        orphan_executor
            .execute(
                TurnInput {
                    operation_id: orphan_operation,
                    input: ModelContent::Text("ordinary input".into()),
                    selected_context: None,
                    max_steps: 1,
                },
                &journal,
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        journal
            .replay(orphan_operation)
            .await?
            .iter()
            .all(|record| !matches!(
                record.event,
                ExecutionEvent::ModelInputPrepared { .. }
                    | ExecutionEvent::ModelStarted { .. }
                    | ExecutionEvent::Model { .. }
            ))
    );
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
