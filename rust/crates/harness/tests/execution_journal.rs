//! Ref-only durable executor observations across Stream and Filesystem.
#![cfg(feature = "filesystem")]
#![allow(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    reason = "ordered journal scenarios index the entries they appended"
)]

use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_harness::filesystem::{
    FilesystemExecutionJournal, FilesystemHost, FilesystemInteractionHost,
};
use acyclic_harness::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, InteractionId, OperationId, Outcome, Result,
    TaskId,
    context::ContextPipeline,
    conversation::{
        ContentGrant, FileDescriptor, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope},
    durable_tool::DurableToolRunner,
    executor::{
        ExecutionEvent, ExecutionJournal, ExecutionRecord, Executor, StockExecutor,
        ToolFailureKind, TurnInput,
    },
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    model::{
        FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
        ModelRequest, ModelRole, PreparedModelRequest,
    },
    resources::ProviderRef,
    runtime::{Bindings, RuntimeScope, TaskAdmissionRecord, TaskStateProvider, ToolContext},
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_stream::{BoxProviderFuture, MemoryStream};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

async fn filesystem(
    name: &str,
    owner: &str,
    agent: AgentId,
    key: u8,
) -> Result<(
    Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    VolumeRef,
    AuthorityIssuer,
)> {
    let provider = ProviderRef::new(name.into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let private = VolumeRef::new(
        provider,
        "scratch".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new(
        name,
        [key; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: owner.into(),
        },
    );
    Ok((host, private, issuer))
}

fn scope(
    issuer: &AuthorityIssuer,
    private: &VolumeRef,
    agent: AgentId,
    extra: &[String],
) -> Result<Scope> {
    let capabilities = Capabilities::new(
        [
            private.capability(VolumeOperation::Read, &mut None)?,
            private.capability(VolumeOperation::Write, &mut None)?,
        ]
        .into_iter()
        .chain(extra.iter().cloned()),
    );
    issuer.issue("agent", &capabilities, Some(agent), None, &mut None)
}

fn journal(
    stream: Arc<MemoryStream>,
    host: Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    private: &VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>> {
    FilesystemExecutionJournal::new(
        stream,
        host,
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: private,
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: scope,
            maximum_bytes: 4_096,
            io: None,
        },
    )
}

async fn bind_conversation(
    stream: &std::sync::Arc<MemoryStream>,
    issuer: &AuthorityIssuer,
    id: &str,
    agent: AgentId,
) -> Result<()> {
    let mut aggregate = StreamAggregate::open(
        stream,
        (
            Authority {
                kind: AggregateKind::Conversation,
                id: id.into(),
            },
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([30; 16]),
                idempotency_key: IdempotencyKey::new(format!("bind:{id}"))?,
                expected_revision: 0,
                scope: issuer
                    .issue(
                        "owner",
                        &Capabilities::new(["conversation:bind"]),
                        None,
                        None,
                        &mut None,
                    )
                    .expect("scope proof"),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    Ok(())
}

struct NoopTool;

struct CountingNoopTool {
    executions: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

impl ToolExecutor for CountingNoopTool {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolResult { value: Value::Null }) })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
}

/// Simulates a terminal race: the committed winner is observable, but this caller receives the losing CAS outcome and must replay without redispatch.
struct TerminalCasLoser {
    inner: Arc<dyn ExecutionJournal>,
    losses: AtomicUsize,
}

impl ExecutionJournal for TerminalCasLoser {
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::executor::ExecutionClaim<'a>> {
        self.inner.claim_execution(operation, io)
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.inner.replay(operation, after, maximum, io)
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.append(operation, key, event, io)
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, bool>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            let terminal = matches!(
                &event,
                ExecutionEvent::ToolCompleted { .. } | ExecutionEvent::ToolFailed { .. }
            );
            let committed = self
                .inner
                .append_if_tail(operation, tail, key, event, io)?
                .await?;
            if terminal
                && committed
                && self
                    .losses
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |losses| {
                        (losses < 2).then_some(losses + 1)
                    })
                    .is_ok()
            {
                return Ok(false);
            }
            Ok(committed)
        })
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media_type, io)
    }

    fn load<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        self.inner.load(file, io)
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>>
    {
        self.inner.interaction_outcome(id, io)
    }
}

#[derive(Clone, Copy)]
enum OversizedArtifact {
    Invocation,
    Result,
    Projection,
}

/// Injects one oversized custody descriptor without changing the request digest, journal order, invocation values or original completed-body identities.
struct CompletedDescriptorSpy {
    inner: Arc<dyn ExecutionJournal>,
    completed: Vec<FileRef>,
    invocation: FileRef,
    target: OversizedArtifact,
    oversized_bytes: u64,
    completed_reads: AtomicUsize,
    invocation_reads: AtomicUsize,
}

impl ExecutionJournal for CompletedDescriptorSpy {
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::executor::ExecutionClaim<'a>> {
        self.inner.claim_execution(operation, io)
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            let mut records = self.inner.replay(operation, after, maximum, io)?.await?;
            for record in &mut records {
                let reference = match (&mut record.event, self.target) {
                    (
                        ExecutionEvent::ToolStarted { invocation, .. },
                        OversizedArtifact::Invocation,
                    ) => invocation,
                    (ExecutionEvent::ToolCompleted { result, .. }, OversizedArtifact::Result) => {
                        result
                    }
                    (
                        ExecutionEvent::ToolCompleted { projection, .. },
                        OversizedArtifact::Projection,
                    ) => projection,
                    _ => continue,
                };
                *reference = FileRef::new(
                    reference.volume().clone(),
                    reference.path().into(),
                    reference.version().into(),
                    FileDescriptor::new(
                        *reference.descriptor().sha256(),
                        self.oversized_bytes,
                        reference.descriptor().media_type().into(),
                    )?,
                    reference.display_name().into(),
                    &mut None,
                )?;
            }
            Ok(records)
        })
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.append(operation, key, event, io)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media, io)
    }
    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        if self.completed.iter().any(|original| {
            original.volume() == reference.volume()
                && original.path() == reference.path()
                && original.version() == reference.version()
        }) {
            self.completed_reads.fetch_add(1, Ordering::SeqCst);
        }
        if self.invocation.volume() == reference.volume()
            && self.invocation.path() == reference.path()
            && self.invocation.version() == reference.version()
        {
            self.invocation_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.load(reference, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>>
    {
        self.inner.interaction_outcome(id, io)
    }
}

struct TestTaskState(RuntimeScope);
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TaskStateProvider for TestTaskState {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    async fn observe_admission(&self, _: TaskId) -> Result<TaskAdmissionRecord> {
        Err(Error::Unsupported("not used by this journal test".into()))
    }
    async fn resume_scope(&self, _: TaskId, _: OperationId) -> Result<RuntimeScope> {
        Ok(self.0.clone())
    }
    async fn outcome(&self, _: TaskId) -> Result<Option<Outcome<Value>>> {
        Ok(None)
    }
    async fn cancel(&self, _: TaskId) -> Result<()> {
        Ok(())
    }
}

impl ToolExecutor for NoopTool {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Ok(ToolResult { value: Value::Null }) })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

struct FixedProjection(Value);
impl ToolProjection for FixedProjection {
    fn project(
        &self,
        _: &ToolInvocation,
        _: &ToolResult,
        work: acyclic_harness::NativeWork<'_>,
    ) -> Result<(Value, acyclic_harness::NativeCustody)> {
        self.supports_native_projection(work)?;
        Ok((self.0.clone(), None))
    }
}

#[path = "execution_journal_cases/dispatch.rs"]
mod dispatch;

#[path = "execution_journal_cases/tools.rs"]
mod tools;

#[path = "execution_journal_cases/model.rs"]
mod model;

#[path = "execution_journal_cases/interactions.rs"]
mod interactions;

#[path = "execution_journal_cases/install.rs"]
mod install;

#[path = "execution_journal_cases/input.rs"]
mod input;
