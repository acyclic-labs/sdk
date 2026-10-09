//! Fully replaceable turn execution and the stock streaming model/tool loop.

use crate::contract::capability;
use crate::obs::{obs_span, traced};
use crate::{
    Error, InteractionId, OperationId, Result, TaskId,
    context::{ContextInput, ContextPipeline},
    conversation::{Attachment, FileRef, Limits, VolumeClass},
    interaction::{Interaction, InteractionOutcome},
    model::{
        Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
        ModelProvider, ModelRequest, ModelRole,
    },
    projection::SelectedModelContext,
    registry::ComponentIdentity,
    runtime::{
        DurableTaskHost, RuntimeScope, ToolPolicy, ToolPolicyDecision, check_tool_approval,
        tool_approval_request, validate_policy_identity,
    },
    tool::{ToolInvocation, ToolRegistry, ToolResult, validate_value},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

/// Internal replay batch size; callers may request any positive page allowance.
pub(crate) const EXECUTION_REPLAY_PAGE_RECORDS: u32 = 64;

pub(crate) fn validate_execution_page(_after: u64, maximum: u32) -> Result<()> {
    if maximum == 0 {
        return Err(Error::Invalid(
            "execution replay page bound is invalid".into(),
        ));
    }
    Ok(())
}

/// Durable input to any custom executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnInput {
    /// Stable durable turn execution identity.
    pub operation_id: OperationId,
    /// Typed provider-neutral input; canonical conversation storage still uses file refs.
    pub input: ModelContent,
    /// Exact, separately recorded canonical history selection for this turn.
    /// When present its last user message is the current `input`; the stock
    /// context pipeline does not synthesize a duplicate user message.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Maximum model/tool steps permitted for this turn.
    pub max_steps: u32,
}

impl TurnInput {
    /// Constructs a turn from an exact, already recorded conversation
    /// selection. The final selected user message is the turn input, so no
    /// text is copied from a conversation event into a durable request.
    pub fn from_selected_context(
        operation_id: OperationId,
        selected_context: SelectedModelContext,
        max_steps: u32,
    ) -> Result<Self> {
        let input = selected_context
            .messages
            .last()
            .filter(|message| message.role == ModelRole::User)
            .map(|message| message.content.clone())
            .ok_or_else(|| {
                Error::Invalid("selected context must end with a user message".into())
            })?;
        input.validate_user_input()?;
        selected_context.validate_for_input(&input)?;
        Ok(Self {
            operation_id,
            input,
            selected_context: Some(selected_context),
            max_steps,
        })
    }
}

/// Gapless replay record returned by a durable execution journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Gapless one-based journal sequence.
    pub sequence: u64,
    /// Stable per-step retry identity.
    pub idempotency_key: String,
    /// Canonical observation.
    pub event: ExecutionEvent,
}

/// Canonical executor observation suitable for a durable journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "journal observations preserve direct typed ref fields"
)]
pub enum ExecutionEvent {
    /// Binds an operation identity to one immutable request and composition.
    Started {
        /// Digest of the input, stock executor version, model, stages, and tools.
        request_digest: [u8; 32],
    },
    /// A model request identity committed before provider dispatch.
    ModelStarted {
        /// Zero-based executor step.
        step: u32,
        /// Digest of the exact model request.
        request_digest: [u8; 32],
        /// Pinned private artifact containing the exact provider-neutral request bytes.
        request: FileRef,
    },
    /// One model stream item was observed.
    Model {
        /// Zero-based executor step.
        step: u32,
        /// Pinned, private JSON file containing one observed model event.
        event: FileRef,
    },
    /// Tool dispatch is about to begin.
    ToolStarted {
        /// Zero-based executor step.
        step: u32,
        /// Stable provider/model-owned call identity.
        call_id: String,
        /// Pinned, private JSON file containing the admitted invocation.
        invocation: FileRef,
    },
    /// Tool execution and projection completed.
    ToolCompleted {
        /// Zero-based executor step.
        step: u32,
        /// Stable provider/model-owned call identity.
        call_id: String,
        /// Pinned private JSON file containing the validated result.
        result: FileRef,
        /// Pinned private JSON file containing the model-visible projection.
        projection: FileRef,
    },
    /// A terminal tool failure recorded without exception bodies or secrets.
    ToolFailed {
        /// Zero-based executor step.
        step: u32,
        /// Stable call identity.
        call_id: String,
        /// Bounded classification; raw provider errors never enter the journal.
        reason: ToolFailureKind,
    },
}

/// Stable, non-secret terminal tool failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolFailureKind {
    /// Executor rejected the already admitted call.
    ExecutorRejected,
    /// Executor result did not satisfy its pinned output contract.
    InvalidOutput,
    /// A model-visible result could not be projected safely.
    ProjectionRejected,
    /// A result artifact could not be published under the pinned limits.
    PublicationRejected,
}

impl ToolFailureKind {
    /// Credential-free, stable diagnostic for a recorded tool failure.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::ExecutorRejected => "tool executor rejected the admitted call",
            Self::InvalidOutput => "tool output violated its pinned schema",
            Self::ProjectionRejected => "tool result projection was rejected",
            Self::PublicationRejected => "tool result artifact could not be published",
        }
    }
}

/// Durable host services available to an executor; policy remains executor-owned.
pub trait ExecutionJournal: acyclic_stream::ProviderPlatform {
    /// Implementations must scope sequences and retry keys by `operation_id`.
    /// Reads at most `maximum` records after the exclusive one-based sequence
    /// `after`. Zero starts at the first record. A provider may return a shorter
    /// internal batch than requested; no complete-history fallback is provided.
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>>;

    /// Appends one reconstructable executor observation.
    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>>;

    /// Atomically appends a dispatch or terminal observation at an exact
    /// journal tail. A claimant may execute or publish only when this returns
    /// true; false means another host advanced the journal. Providers unable
    /// to offer a linearizable compare-and-append must fail closed.
    fn append_if_tail<'a>(
        &'a self,
        _operation_id: OperationId,
        _expected_tail: u64,
        _claim_id: String,
        _event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "atomic execution journal append is unavailable".into(),
            ))
        })
    }

    /// Stages immutable private bytes before any referring observation is appended.
    /// Retries with the same key and different bytes must fail closed.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>>;

    /// Reads an exact version under the journal owner's grant and verifies its descriptor.
    fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>>;

    /// Verifies a user-supplied file under its owner-mediated grant before turn admission.
    fn verify_input_file<'a>(&'a self, _reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "turn input file verification is unavailable".into(),
            ))
        })
    }

    /// Verifies data and native option bindings without granting fresh execution.
    fn verify_model_content<'a>(&'a self, content: &'a ModelContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if !content.native_configurations().is_empty() {
                return Err(Error::Unsupported(
                    "original native option verification is unavailable".into(),
                ));
            }
            for file in content.file_refs() {
                self.verify_input_file(file).await?;
            }
            Ok(())
        })
    }

    /// Confirms the selected model context is the exact projection of the
    /// owning conversation's previously committed selection for this turn.
    fn verify_selected_context<'a>(
        &'a self,
        _operation_id: OperationId,
        _selected: &'a SelectedModelContext,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "canonical model context verification is unavailable".into(),
            ))
        })
    }

    /// Opens one typed durable interaction exactly once.
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>>;

    /// Reads the canonical durable outcome without treating refusal as an answer.
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>>;
}

/// Incremental replay validation retaining only a cursor and retry digests.
/// Pages are returned to the caller and are never retained by this cursor.
pub struct ExecutionReplay {
    operation_id: OperationId,
    after: u64,
    keys: BTreeSet<[u8; 32]>,
}

impl ExecutionReplay {
    /// Starts validation at the first observation of one exact execution.
    #[must_use]
    pub fn new(operation_id: OperationId) -> Self {
        Self {
            operation_id,
            after: 0,
            keys: BTreeSet::new(),
        }
    }

    /// Last successfully validated one-based sequence, zero for an empty journal.
    #[must_use]
    pub const fn tail(&self) -> u64 {
        self.after
    }

    /// Reads one bounded page, or `None` at the current tail. Invalid pages do
    /// not advance the cursor.
    pub async fn next_page(
        &mut self,
        journal: &dyn ExecutionJournal,
    ) -> Result<Option<Vec<ExecutionRecord>>> {
        validate_execution_page(self.after, EXECUTION_REPLAY_PAGE_RECORDS)?;
        let page = journal
            .replay(self.operation_id, self.after, EXECUTION_REPLAY_PAGE_RECORDS)
            .await?;
        if page.len() > EXECUTION_REPLAY_PAGE_RECORDS as usize {
            return Err(Error::Storage("execution replay page exceeds bound".into()));
        }
        if page.is_empty() {
            return Ok(None);
        }
        let mut keys = BTreeSet::new();
        for (index, record) in page.iter().enumerate() {
            let key = *blake3::hash(record.idempotency_key.as_bytes()).as_bytes();
            if record.operation_id != self.operation_id
                || record.sequence != self.after + index as u64 + 1
                || record.idempotency_key.is_empty()
                || self.keys.contains(&key)
                || !keys.insert(key)
            {
                return Err(Error::Conflict(
                    "execution journal identity or sequence is invalid".into(),
                ));
            }
        }
        self.after += page.len() as u64;
        self.keys.extend(keys);
        Ok(Some(page))
    }
}

pub(crate) async fn replay_execution(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    maximum_selected: usize,
    select: impl Fn(&ExecutionEvent) -> bool,
) -> Result<(u64, Vec<ExecutionRecord>)> {
    let mut replay = ExecutionReplay::new(operation_id);
    let mut selected = Vec::new();
    while let Some(page) = replay.next_page(journal).await? {
        for record in page {
            if select(&record.event) {
                if selected.len() >= maximum_selected {
                    return Err(Error::Invalid(
                        "selected execution records exceed bound".into(),
                    ));
                }
                selected.push(record);
            }
        }
    }
    Ok((replay.tail(), selected))
}

/// Terminal result produced by an executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnOutput {
    /// User-visible assistant text.
    pub text: String,
    /// Ordered, already staged assistant attachments or published artifacts.
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// Provider-owned final metadata.
    pub metadata: Value,
    /// Number of completed model steps.
    pub steps: u32,
}

pub(crate) enum StockTurnProgress {
    Ready(TurnOutput),
    Pending(OperationId),
    Rejected(crate::InteractionRejection),
}

impl StockTurnProgress {
    pub(crate) fn into_output(self) -> Result<TurnOutput> {
        match self {
            Self::Ready(output) => Ok(output),
            Self::Pending(operation) => Err(Error::Indeterminate(operation)),
            Self::Rejected(reason) => Err(Error::InteractionRejected(reason)),
        }
    }
}

enum ToolCallProgress {
    Settled,
    Pending(OperationId),
    Rejected(crate::InteractionRejection),
}

/// Complete replaceable turn loop. Implementations may own every policy decision.
pub trait Executor: acyclic_stream::ProviderPlatform {
    /// Executes or resumes one turn using only explicit durable host services.
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>>;
}

/// Complete default streaming model/tool loop assembled from replaceable values.
#[derive(Clone)]
pub struct StockExecutor {
    model: Model,
    max_output_tokens: Option<u32>,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
    limits: Limits,
    tool_scope: RuntimeScope,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
    task: Option<(
        Arc<dyn DurableTaskHost>,
        TaskId,
        crate::scheduler::LeaseFence,
    )>,
    inherited_prefix: Option<(
        FileRef,
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
    )>,
    task_context: Option<(crate::runtime::TaskContext, OperationId)>,
}

impl StockExecutor {
    /// Creates the stock loop without installing hidden stages or tools.
    #[must_use]
    pub fn new(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Self {
        Self {
            model,
            max_output_tokens: None,
            provider,
            context,
            tools,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
            task: None,
            inherited_prefix: None,
            task_context: None,
        }
    }

    /// Sets the caller's output token budget for each model request.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid("model output token budget is zero".into()));
        }
        self.max_output_tokens = Some(maximum);
        Ok(self)
    }

    /// Binds this worker's exact admitted task and execution fence. Fresh model
    /// attempts consume the existing owner journal before dispatch; replay and
    /// reconciliation do not consume another unit.
    #[must_use]
    pub fn with_durable_task(
        mut self,
        host: Arc<dyn DurableTaskHost>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Self {
        self.task = Some((host, task_id, fence));
        self
    }

    /// Applies the composition's checked bounds to the stock loop.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Binds a child to an immutable parent model prefix. The supplied reader
    /// must authenticate the child's exact generation-pinned read grants.
    /// Context stages assemble local input before this prefix is prepended.
    pub fn with_inherited_prefix(
        mut self,
        prefix: FileRef,
        verifier: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    ) -> Result<Self> {
        prefix.validate()?;
        if prefix.descriptor().media_type() != crate::model::ModelPrefix::MEDIA_TYPE {
            return Err(Error::Invalid(
                "inherited model prefix has the wrong media type".into(),
            ));
        }
        self.inherited_prefix = Some((prefix, verifier));
        Ok(self)
    }

    /// Freezes the exact retained model input through a completed tool exchange.
    /// Fork composition may publish this prefix before activating children. This
    /// only reads the existing execution journal; incomplete calls fail closed.
    pub async fn completed_tool_prefix(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        call_id: &str,
    ) -> Result<crate::model::PreparedModelRequest> {
        self.verify_task_context_binding(input.operation_id)?;
        let records = self
            .prefix_records(journal, input.operation_id, step)
            .await?;
        let identity = self.request_digest(input)?;
        if !matches!(records.first(), Some(record) if record.sequence == 1 &&
            matches!(&record.event, ExecutionEvent::Started { request_digest } if request_digest == &identity))
        {
            return Err(Error::Conflict(
                "tool prefix has another execution identity".into(),
            ));
        }
        let retained = retained_model_step(journal, &records, step, self.limits).await?;
        let prepared = retained
            .request
            .ok_or(Error::Indeterminate(input.operation_id))?;
        let mut request = prepared.request().clone();
        let mut calls = Vec::new();
        for event in retained.events {
            if let ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } = event
            {
                calls.push(ToolInvocation::for_model_call(
                    input.operation_id,
                    step,
                    call_id,
                    name,
                    arguments,
                ));
            }
        }
        if !retained.admission.completed || !calls.iter().any(|call| call.call_id == call_id) {
            return Err(Error::Indeterminate(input.operation_id));
        }
        for invocation in calls {
            let projection =
                completed_tool_projection(journal, &records, step, &invocation, &request.tools)
                    .await?;
            request.messages.extend([
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: invocation.call_id.clone(),
                        name: invocation.name.clone(),
                        arguments: invocation.arguments,
                    }),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: invocation.call_id.clone(),
                        name: invocation.name,
                        content: serde_json::from_value(projection)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    }),
                },
            ]);
            if invocation.call_id == call_id {
                break;
            }
        }
        let prepared = crate::model::PreparedModelRequest::prepare(request, self.limits)?;
        self.verify_model_request_content(journal, &prepared)
            .await?;
        Ok(prepared)
    }

    /// Enforces the same explicit tool grants and policy in the stock model loop.
    pub fn with_tool_authority(
        mut self,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        if let Some(policy) = &policy {
            validate_policy_identity(&policy.identity())?;
        }
        self.policy_identity = policy.as_ref().map(|policy| policy.identity());
        self.tool_scope = scope;
        self.policy = policy;
        Ok(self)
    }

    /// Carries the original admitted context into one bound stock execution.
    /// Context does not replace the durable host's current fence verification.
    pub fn with_task_context(
        mut self,
        context: &crate::runtime::TaskContext,
        execution_operation: OperationId,
    ) -> Result<Self> {
        let context = context.scoped(self.tool_scope.grants().clone(), self.limits)?;
        self.task_context = Some((context, execution_operation));
        self.verify_task_context_binding(execution_operation)?;
        Ok(self)
    }

    fn verify_task_context_binding(&self, operation: OperationId) -> Result<()> {
        if let Some((context, expected_operation)) = &self.task_context
            && (*expected_operation != operation
                || context.durable_task_id() != self.task.as_ref().map(|(_, task, _)| *task)
                || context.scope().grants() != self.tool_scope.grants()
                || context.scope().limits() != self.limits
                || context.scope().run_limits() != self.tool_scope.run_limits()
                || context.scope().extensions() != self.tool_scope.extensions())
        {
            return Err(Error::Unauthorized(
                "stock task context differs from execution binding".into(),
            ));
        }
        Ok(())
    }

    fn invocation_context(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<crate::runtime::ToolContext>> {
        self.task_context
            .as_ref()
            .map(|(context, _)| {
                crate::runtime::ToolContext::new(
                    context.clone(),
                    invocation.operation_id,
                    invocation.call_id.clone(),
                )
            })
            .transpose()
    }

    fn request_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        let mut request = json!({
            "executor": "acyclic.stock.v4",
            "input": input,
            "model": self.model,
            "max_output_tokens": self.max_output_tokens,
            "context": self.context.contracts(),
            "tools": self.tools.definitions()?,
            "limits": self.limits,
            "tool_scope": (self.tool_scope.grants(), self.tool_scope.limits()),
            "policy": self.policy_identity.as_ref(),
            "inherited_prefix": self.inherited_prefix.as_ref().map(|(reference, _)| reference),
        });
        if let Some((_, task_id, _)) = &self.task {
            request
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("executor request is not an object".into()))?
                .insert("task_id".into(), json!(task_id));
        }
        if let Some((context, execution_operation)) = &self.task_context {
            request
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("executor request is not an object".into()))?
                .insert(
                    "task_context".into(),
                    json!({
                        "admission_operation": context.id(),
                        "execution_operation": execution_operation,
                        "task_id": context.durable_task_id(),
                        "run_limits": context.scope().run_limits(),
                        "extensions": context.scope().extensions(),
                    }),
                );
        }
        crate::contract::canonical_json_digest(&request)
    }

    /// Replays the durable journal for one turn, verifying it is gapless and bound to the
    /// exact same request, and journals the initial `Started` marker on a fresh turn.
    async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.verify_task_context_binding(input.operation_id)?;
        let (tail, records) = replay_execution(journal, input.operation_id, 1, |event| {
            matches!(event, ExecutionEvent::Started { .. })
        })
        .await?;
        if tail != 0 && records.first().is_none_or(|record| record.sequence != 1) {
            return Err(Error::Conflict(
                "execution journal has no initial request binding".into(),
            ));
        }
        let request_digest = self.request_digest(input)?;
        match records.first().map(|record| &record.event) {
            Some(ExecutionEvent::Started {
                request_digest: existing,
            }) if existing == &request_digest => {}
            Some(_) => {
                return Err(Error::Conflict(
                    "execution identity is bound to another request or configuration".into(),
                ));
            }
            None => {
                journal
                    .append(
                        input.operation_id,
                        "execution:started".into(),
                        ExecutionEvent::Started { request_digest },
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// Runs one admitted model step independently of the stock control loop.
    /// Custom loops reuse exact artifacts, validation, reconciliation and durable
    /// observations. A started attempt never reruns sources or redispatches effects.
    pub async fn model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        self.verify_task_context_binding(input.operation_id)?;
        self.validate_turn_input(journal, input).await?;
        if step >= input.max_steps {
            return Err(Error::Invalid(
                "model step is outside admitted turn bounds".into(),
            ));
        }
        self.ensure_started(journal, input).await?;
        self.run_model_step(journal, input, step, prior_messages)
            .await
    }

    async fn run_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        let span = obs_span!(
            "acyclic.harness.executor.model_step",
            step = step,
            model = self.model.name.as_str(),
            phase = crate::obs::Empty,
            items = crate::obs::Empty,
        );
        traced(
            span,
            self.dispatch_model_step(journal, input, step, prior_messages),
        )
        .await
    }

    /// Resolves one model step's events, replaying an already completed or started attempt
    /// from the durable journal exactly once instead of re-invoking the provider.
    #[allow(
        clippy::too_many_lines,
        reason = "one exactly-once replay-or-generate operation for a single model step \
                  (already-completed replay, in-flight reconcile, or fresh generate, each \
                  interleaved with journal appends); splitting the branches further would \
                  fragment one atomic step across more functions without clarifying it"
    )]
    async fn dispatch_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        if let Some((host, task_id, fence)) = &self.task {
            host.verify_execution_owner(*task_id, fence.clone()).await?;
        }
        let (_, records) = self
            .model_records(journal, input.operation_id, step)
            .await?;
        let RetainedModelStep {
            request: retained_request,
            mut admission,
            events: replayed_model,
        } = retained_model_step(journal, &records, step, self.limits).await?;
        let started = retained_request.is_some();
        let request = if let Some(recorded) = retained_request {
            recorded
        } else {
            let context = self
                .context
                .run_bounded(
                    &ContextInput {
                        input: input.input.clone(),
                        selected_context: input.selected_context.clone(),
                        step,
                        prior_messages: prior_messages.to_vec(),
                    },
                    self.limits,
                )
                .await?;
            let request = ModelRequest {
                model: self.model.clone(),
                messages: context.messages,
                tools: self
                    .tools
                    .definitions()?
                    .into_iter()
                    .filter(|tool| {
                        self.tool_scope
                            .grants()
                            .contains(&capability::tool_call(&tool.name))
                    })
                    .collect(),
                max_output_tokens: self.max_output_tokens,
            };
            if let Some((prefix, verifier)) = &self.inherited_prefix {
                crate::model::PreparedModelRequest::inherit(
                    request,
                    prefix,
                    verifier.as_ref(),
                    self.limits,
                )
                .await?
            } else {
                crate::model::PreparedModelRequest::prepare(request, self.limits)?
            }
        };
        self.verify_model_request_content(journal, &request).await?;
        let request_digest = request.manifest().request_digest;
        let replay_completed = admission.completed;
        crate::obs::obs_record!(
            "phase" = match (replay_completed, started) {
                (true, _) => "replay",
                (false, true) => "reconcile",
                (false, false) => "dispatch",
            }
        );
        let model_events = if replay_completed {
            replayed_model
        } else if started {
            let Some(mut continuation) = self
                .provider
                .reconcile(ModelAttempt {
                    operation_id: input.operation_id,
                    step,
                    request_digest,
                    observed: replayed_model.clone(),
                })
                .await?
            else {
                return Err(Error::Indeterminate(input.operation_id));
            };
            let mut observed = replayed_model;
            for event in continuation.drain(..) {
                admission.observe(&event, self.limits)?;
                let key = format!("model:{step}:{}", observed.len());
                let reference = stage_json(journal, input.operation_id, &key, &event).await?;
                journal
                    .append(
                        input.operation_id,
                        key,
                        ExecutionEvent::Model {
                            step,
                            event: reference,
                        },
                    )
                    .await?;
                observed.push(event);
            }
            observed
        } else {
            let (tail, current) = self
                .model_records(journal, input.operation_id, step)
                .await?;
            if let Some(existing) = current.iter().find_map(|record| match &record.event {
                ExecutionEvent::ModelStarted {
                    step: event_step,
                    request_digest,
                    ..
                } if *event_step == step => Some(*request_digest),
                _ => None,
            }) {
                if existing != request_digest {
                    return Err(Error::Conflict(
                        "model attempt identity is bound to another request".into(),
                    ));
                }
                return Err(Error::Indeterminate(input.operation_id));
            }
            let request_ref = stage_bytes(
                journal,
                input.operation_id,
                &format!("model:{step}:request"),
                request.bytes().to_vec(),
            )
            .await?;
            if let Some((host, task_id, fence)) = &self.task {
                host.claim_model_dispatch(
                    *task_id,
                    input.operation_id,
                    step,
                    request_digest,
                    fence.clone(),
                )
                .await?;
            }
            let claimed = journal
                .append_if_tail(
                    input.operation_id,
                    tail,
                    format!("model:{step}:claim:{}", OperationId::new()),
                    ExecutionEvent::ModelStarted {
                        step,
                        request_digest,
                        request: request_ref,
                    },
                )
                .await;
            match claimed {
                Ok(true) => {}
                Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                Err(error) => return Err(error),
            }
            let mut stream = self.provider.generate(
                request,
                crate::model::ModelDispatch {
                    operation_id: input.operation_id,
                    step,
                    request_digest,
                },
            );
            let mut observed = Vec::new();
            while let Some(event) = stream.next().await {
                let event = event?;
                admission.observe(&event, self.limits)?;
                let key = format!("model:{step}:{}", observed.len());
                let reference = stage_json(journal, input.operation_id, &key, &event).await?;
                journal
                    .append(
                        input.operation_id,
                        key,
                        ExecutionEvent::Model {
                            step,
                            event: reference,
                        },
                    )
                    .await?;
                observed.push(event);
            }
            observed
        };
        crate::obs::obs_record!("items" = model_events.len());
        Ok(model_events)
    }

    async fn prefix_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
    ) -> Result<Vec<ExecutionRecord>> {
        let maximum = self
            .limits
            .model_events_per_step
            .saturating_add(self.limits.tool_calls_per_step.saturating_mul(3))
            .saturating_add(2);
        let (_, records) = replay_execution(journal, operation_id, maximum, |event| match event {
            ExecutionEvent::Started { .. } => true,
            ExecutionEvent::ModelStarted { step: recorded, .. }
            | ExecutionEvent::Model { step: recorded, .. }
            | ExecutionEvent::ToolStarted { step: recorded, .. }
            | ExecutionEvent::ToolCompleted { step: recorded, .. }
            | ExecutionEvent::ToolFailed { step: recorded, .. } => *recorded == step,
        })
        .await?;
        Ok(records)
    }

    async fn model_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(
            journal,
            operation_id,
            self.limits.model_events_per_step.saturating_add(1),
            |event| {
                matches!(event,
                ExecutionEvent::ModelStarted { step: event_step, .. }
                | ExecutionEvent::Model { step: event_step, .. } if *event_step == step)
            },
        )
        .await
    }

    async fn tool_records(
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        call_id: &str,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(journal, operation_id, 3, |event| {
            matches!(event,
            ExecutionEvent::ToolStarted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolCompleted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolFailed { step: event_step, call_id: existing, .. }
            if *event_step == step && existing == call_id)
        })
        .await
    }

    async fn record_tool_failure(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        call_id: &str,
        reason: ToolFailureKind,
    ) -> Result<()> {
        let (tail, current) = Self::tool_records(journal, operation_id, step, call_id).await?;
        if current.iter().any(|record| {
            matches!(&record.event,
            ExecutionEvent::ToolCompleted { step: event_step, call_id: existing, .. }
                | ExecutionEvent::ToolFailed { step: event_step, call_id: existing, .. }
                if *event_step == step && existing == call_id)
        }) {
            return Err(Error::Indeterminate(operation_id));
        }
        match journal
            .append_if_tail(
                operation_id,
                tail,
                format!("tool:{step}:{call_id}:failed:{}", OperationId::new()),
                ExecutionEvent::ToolFailed {
                    step,
                    call_id: call_id.into(),
                    reason,
                },
            )
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                Err(Error::Indeterminate(operation_id))
            }
            Err(error) => Err(error),
        }
    }

    async fn resolve_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ToolCallProgress> {
        let span = obs_span!("acyclic.harness.executor.tool_call", step = step);
        let call = self.settle_tool_call(journal, operation_id, step, invocation, prior_messages);
        traced(span, call).await
    }

    /// Resolves one tool invocation against the durable journal, replaying an already
    /// completed or started attempt exactly once, and appends the resulting message.
    #[allow(
        clippy::too_many_lines,
        reason = "one exactly-once replay-or-execute operation for a single tool call \
                  (already-completed replay, in-flight reconcile, or fresh execute, each \
                  interleaved with journal appends); splitting the branches further would \
                  fragment one atomic invocation across more functions without clarifying it"
    )]
    async fn settle_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ToolCallProgress> {
        self.verify_task_context_binding(operation_id)?;
        invocation.validate()?;
        let tool = self
            .tools
            .get(&invocation.name)
            .ok_or_else(|| Error::NotFound(format!("tool {}", invocation.name)))?;
        // Authorization precedes argument validation, and must stay that way. A validation error
        // describes the tool's pinned input schema, so answering one for a tool the caller was
        // never granted would let a model probe the contract of an ungranted tool by naming it
        // with deliberately malformed arguments. An ungranted call is refused on its own terms,
        // whatever its arguments look like.
        let capability = capability::tool_call(&tool.definition.name);
        if !self.tool_scope.grants().contains(&capability) {
            return Err(Error::Unauthorized(format!("scope lacks {capability}")));
        }
        crate::contract::validate_json_byte_bound(
            &invocation,
            self.limits
                .file_bytes
                .min(self.tool_scope.limits().file_bytes),
        )?;
        if let Some(context) = self.invocation_context(&invocation)? {
            tool.executor
                .authorize_with_context(&context, &invocation)?;
        } else {
            tool.executor
                .authorize(Some(&self.tool_scope), &invocation)?;
        }
        validate_value(
            &tool.definition.input_schema,
            &invocation.arguments,
            "tool input",
        )?;
        let (_, records) =
            Self::tool_records(journal, operation_id, step, &invocation.call_id).await?;
        let started = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted {
                step: event_step,
                call_id,
                invocation: existing,
            } if *event_step == step && call_id == &invocation.call_id => Some(existing),
            _ => None,
        });
        if let Some(existing) = started
            && load_json::<ToolInvocation>(journal, existing).await? != invocation
        {
            return Err(Error::Conflict(
                "tool call identity is bound to another invocation".into(),
            ));
        }
        if let Some(reason) = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolFailed {
                step: event_step,
                call_id,
                reason,
            } if *event_step == step && call_id == &invocation.call_id => Some(*reason),
            _ => None,
        }) {
            return Err(Error::Invalid(reason.message().into()));
        }
        let completed_tool = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                step: event_step,
                call_id,
                result,
                projection,
            } if *event_step == step && call_id == &invocation.call_id => {
                Some((result.clone(), projection.clone()))
            }
            _ => None,
        });
        if completed_tool.is_some() && started.is_none() {
            return Err(Error::Invalid(
                "completed tool has no admitted invocation".into(),
            ));
        }
        let (result, projection) = if let Some(completed) = completed_tool {
            (
                load_json::<ToolResult>(journal, &completed.0).await?,
                load_json::<Value>(journal, &completed.1).await?,
            )
        } else {
            {
                let scope = &self.tool_scope;
                if let Some(policy) = &self.policy {
                    if self.policy_identity.as_ref() != Some(&policy.identity()) {
                        return Err(Error::Conflict("stock policy changed after binding".into()));
                    }
                    let decision = policy.evaluate(&invocation, scope).await?;
                    if self.policy_identity.as_ref() != Some(&policy.identity()) {
                        return Err(Error::Conflict(
                            "stock policy changed during evaluation".into(),
                        ));
                    }
                    match decision {
                        ToolPolicyDecision::Allow => {}
                        ToolPolicyDecision::Deny { reason } => {
                            return Err(Error::Unauthorized(reason));
                        }
                        ToolPolicyDecision::RequireApproval { prompt } => {
                            if !scope.grants().contains(capability::INTERACTION_ROUTE) {
                                return Err(Error::Unauthorized(
                                    "stock tool approval requires interaction:route".into(),
                                ));
                            }
                            let task = self.task.as_ref().map_or(
                                TaskId::from_bytes(operation_id.into_bytes()),
                                |(_, task, _)| *task,
                            );
                            let (approval_operation, request) = tool_approval_request(
                                task,
                                self.policy_identity.as_ref().ok_or_else(|| {
                                    Error::Conflict("stock policy identity is missing".into())
                                })?,
                                &tool.definition,
                                &invocation,
                                prompt,
                            )?;
                            let approval =
                                crate::durable_host::task_interaction_id(task, approval_operation);
                            journal.open_interaction(approval, request).await?;
                            let Some(outcome) = journal.interaction_outcome(approval).await? else {
                                if started.is_some() {
                                    return Err(Error::Indeterminate(invocation.operation_id));
                                }
                                return Ok(ToolCallProgress::Pending(invocation.operation_id));
                            };
                            match check_tool_approval(outcome) {
                                Ok(()) => {}
                                Err(Error::InteractionRejected(reason)) if started.is_none() => {
                                    return Ok(ToolCallProgress::Rejected(reason));
                                }
                                Err(Error::InteractionRejected(_)) => {
                                    return Err(Error::Indeterminate(invocation.operation_id));
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                }
            }
            let claimed = if started.is_some() {
                false
            } else {
                let invocation_ref = stage_json(
                    journal,
                    operation_id,
                    &format!("tool:{step}:{}:invocation", invocation.call_id),
                    &invocation,
                )
                .await?;
                let (tail, current) =
                    Self::tool_records(journal, operation_id, step, &invocation.call_id).await?;
                if current.iter().any(|record| {
                    matches!(&record.event,
                    ExecutionEvent::ToolStarted { step: event_step, call_id, .. }
                        if *event_step == step && call_id == &invocation.call_id)
                }) {
                    return Err(Error::Indeterminate(operation_id));
                }
                match journal
                    .append_if_tail(
                        operation_id,
                        tail,
                        format!(
                            "tool:{step}:{}:claim:{}",
                            invocation.call_id,
                            OperationId::new()
                        ),
                        ExecutionEvent::ToolStarted {
                            step,
                            call_id: invocation.call_id.clone(),
                            invocation: invocation_ref,
                        },
                    )
                    .await
                {
                    Ok(true) => true,
                    Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(error) => return Err(error),
                }
            };
            let tool_context = self.invocation_context(&invocation)?;
            let result = if claimed {
                let execution = if let Some(context) = tool_context {
                    tool.executor
                        .execute_with_context(context, invocation.clone())
                        .await
                } else {
                    tool.executor.execute(invocation.clone()).await
                };
                match execution {
                    Ok(result) => result,
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(_) => {
                        self.record_tool_failure(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ExecutorRejected,
                        )
                        .await?;
                        return Err(Error::Invalid(
                            ToolFailureKind::ExecutorRejected.message().into(),
                        ));
                    }
                }
            } else {
                let reconciliation = if let Some(context) = tool_context {
                    tool.executor
                        .reconcile_with_context(context, invocation.clone())
                        .await
                } else {
                    tool.executor.reconcile(invocation.clone()).await
                };
                match reconciliation {
                    Ok(Some(result)) => result,
                    Ok(None) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(_) => {
                        self.record_tool_failure(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ExecutorRejected,
                        )
                        .await?;
                        return Err(Error::Invalid(
                            ToolFailureKind::ExecutorRejected.message().into(),
                        ));
                    }
                }
            };
            if validate_value(&tool.definition.output_schema, &result.value, "tool output").is_err()
            {
                self.record_tool_failure(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    ToolFailureKind::InvalidOutput,
                )
                .await?;
                return Err(Error::Invalid(
                    ToolFailureKind::InvalidOutput.message().into(),
                ));
            }
            let Ok(projection) = tool
                .projection
                .project(&invocation, &result)
                .and_then(|value| {
                    tool.definition
                        .validate_projection(&value)?
                        .validate_limits(self.limits)?;
                    if crate::contract::canonical_json_bytes(&value)?.len() as u64
                        > self.limits.render_bytes
                    {
                        return Err(Error::Invalid(
                            "tool projection exceeds render limit".into(),
                        ));
                    }
                    Ok(value)
                })
            else {
                self.record_tool_failure(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    ToolFailureKind::ProjectionRejected,
                )
                .await?;
                return Err(Error::Invalid(
                    ToolFailureKind::ProjectionRejected.message().into(),
                ));
            };
            let result_ref = stage_json(
                journal,
                operation_id,
                &format!("tool:{step}:{}:result", invocation.call_id),
                &result,
            )
            .await;
            let result_ref = match result_ref {
                Ok(reference) => reference,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(_) => {
                    self.record_tool_failure(
                        journal,
                        operation_id,
                        step,
                        &invocation.call_id,
                        ToolFailureKind::PublicationRejected,
                    )
                    .await?;
                    return Err(Error::Invalid(
                        ToolFailureKind::PublicationRejected.message().into(),
                    ));
                }
            };
            let projection_ref = stage_json(
                journal,
                operation_id,
                &format!("tool:{step}:{}:projection", invocation.call_id),
                &projection,
            )
            .await;
            let projection_ref = match projection_ref {
                Ok(reference) => reference,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(_) => {
                    self.record_tool_failure(
                        journal,
                        operation_id,
                        step,
                        &invocation.call_id,
                        ToolFailureKind::PublicationRejected,
                    )
                    .await?;
                    return Err(Error::Invalid(
                        ToolFailureKind::PublicationRejected.message().into(),
                    ));
                }
            };
            let (tail, current) =
                Self::tool_records(journal, operation_id, step, &invocation.call_id).await?;
            if current.iter().any(|record| {
                matches!(&record.event,
                ExecutionEvent::ToolCompleted { step: event_step, call_id, .. }
                    | ExecutionEvent::ToolFailed { step: event_step, call_id, .. }
                    if *event_step == step && call_id == &invocation.call_id)
            }) {
                return Err(Error::Indeterminate(operation_id));
            }
            match journal
                .append_if_tail(
                    operation_id,
                    tail,
                    format!(
                        "tool:{step}:{}:completed:{}",
                        invocation.call_id,
                        OperationId::new()
                    ),
                    ExecutionEvent::ToolCompleted {
                        step,
                        call_id: invocation.call_id.clone(),
                        result: result_ref,
                        projection: projection_ref,
                    },
                )
                .await
            {
                Ok(true) => {}
                Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(error) => return Err(error),
            }
            (result, projection)
        };
        validate_value(&tool.definition.output_schema, &result.value, "tool output")?;
        let content = tool.definition.validate_projection(&projection)?;
        let message = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: invocation.call_id.clone(),
                name: invocation.name.clone(),
                content,
            }),
        };
        message.content.validate_limits(self.limits)?;
        prior_messages.push(message);
        Ok(ToolCallProgress::Settled)
    }

    async fn verify_model_content(
        &self,
        journal: &dyn ExecutionJournal,
        content: &ModelContent,
    ) -> Result<()> {
        verify_model_content_scoped(
            journal,
            content,
            self.limits,
            self.task_context
                .as_ref()
                .map(|(context, _)| context.scope()),
        )
        .await
    }

    async fn verify_model_request_content(
        &self,
        journal: &dyn ExecutionJournal,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<()> {
        verify_model_contents_scoped(
            journal,
            request
                .request()
                .messages
                .iter()
                .map(|message| &message.content),
            self.limits,
            self.task_context
                .as_ref()
                .map(|(context, _)| context.scope()),
        )
        .await
    }

    async fn validate_turn_input(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.limits.validate()?;
        if input.max_steps == 0 || input.max_steps as usize > self.limits.model_steps {
            return Err(Error::Invalid(
                "max_steps exceeds configured model step bound".into(),
            ));
        }
        input.input.validate_user_input()?;
        let scope = self
            .task_context
            .as_ref()
            .map(|(context, _)| context.scope());
        validate_model_content_scope(&input.input, self.limits, scope)?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input)?;
            if selected.messages.len() > self.limits.context_messages {
                return Err(Error::Invalid(
                    "selected context exceeds configured message limit".into(),
                ));
            }
            // Reject all structural and attenuated read claims before journal IO.
            for message in &selected.messages {
                validate_model_content_scope(&message.content, self.limits, scope)?;
            }
            journal
                .verify_selected_context(input.operation_id, selected)
                .await?;
            for message in &selected.messages {
                message.content.validate_limits(self.limits)?;
                self.verify_model_content(journal, &message.content).await?;
            }
        }
        self.verify_model_content(journal, &input.input).await?;
        Ok(())
    }
}

impl StockExecutor {
    pub(crate) fn execute_progress<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<StockTurnProgress>> {
        let span = obs_span!(INFO, "acyclic.harness.executor.execute");
        traced(span, async move {
            self.verify_task_context_binding(input.operation_id)?;
            if let Some((host, task_id, fence)) = &self.task {
                host.verify_execution_owner(*task_id, fence.clone()).await?;
            }
            self.validate_turn_input(journal, &input).await?;
            self.ensure_started(journal, &input).await?;
            let mut prior_messages = Vec::new();
            let mut text = String::new();
            for step in 0..input.max_steps {
                let mut calls = Vec::new();
                let mut completed = None;
                // Keep nested durable provider futures within native worker stacks.
                let model_events =
                    Box::pin(self.run_model_step(journal, &input, step, &prior_messages)).await?;
                for event in model_events {
                    match event {
                        ModelEvent::Content { delta } => {
                            if text
                                .len()
                                .checked_add(delta.len())
                                .is_none_or(|size| size as u64 > self.limits.file_bytes)
                            {
                                return Err(Error::Invalid(
                                    "assistant output exceeds file limit".into(),
                                ));
                            }
                            text.push_str(&delta);
                        }
                        ModelEvent::ToolCall {
                            call_id,
                            name,
                            arguments,
                        } => {
                            calls.push(ToolInvocation::for_model_call(
                                input.operation_id,
                                step,
                                call_id,
                                name,
                                arguments,
                            ));
                        }
                        ModelEvent::Completed { metadata } => completed = Some(metadata),
                        ModelEvent::Reasoning { .. } => {}
                    }
                }
                let Some(metadata) = completed else {
                    return Err(Error::Indeterminate(input.operation_id));
                };
                if calls.is_empty() {
                    return Ok(StockTurnProgress::Ready(TurnOutput {
                        text,
                        attachments: Vec::new(),
                        metadata,
                        steps: step + 1,
                    }));
                }
                for invocation in calls {
                    let message = ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Part(ModelContentPart::ToolCall {
                            call_id: invocation.call_id.clone(),
                            name: invocation.name.clone(),
                            arguments: invocation.arguments.clone(),
                        }),
                    };
                    message.content.validate_limits(self.limits)?;
                    prior_messages.push(message);
                    match Box::pin(self.resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        &mut prior_messages,
                    ))
                    .await?
                    {
                        ToolCallProgress::Settled => {}
                        ToolCallProgress::Pending(wait) => {
                            return Ok(StockTurnProgress::Pending(wait));
                        }
                        ToolCallProgress::Rejected(reason) => {
                            return Ok(StockTurnProgress::Rejected(reason));
                        }
                    }
                }
            }
            Err(Error::Conflict("executor step limit reached".into()))
        })
    }
}

impl Executor for StockExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move { self.execute_progress(input, journal).await?.into_output() })
    }
}

pub(crate) async fn completed_tool_projection(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    expected: &ToolInvocation,
    tools: &[crate::tool::ToolDefinition],
) -> Result<Value> {
    let invocation = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted {
                step: recorded,
                call_id,
                invocation,
            } if *recorded == step && call_id == &expected.call_id => Some(invocation),
            _ => None,
        })
        .ok_or(Error::Indeterminate(expected.operation_id))?;
    if load_json::<ToolInvocation>(journal, invocation).await? != *expected {
        return Err(Error::Conflict(
            "completed tool prefix invocation differs".into(),
        ));
    }
    let (result, projection) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                step: recorded,
                call_id,
                result,
                projection,
            } if *recorded == step && call_id == &expected.call_id => Some((result, projection)),
            _ => None,
        })
        .ok_or(Error::Indeterminate(expected.operation_id))?;
    let definition = tools
        .iter()
        .find(|tool| tool.name == expected.name)
        .ok_or_else(|| Error::Invalid("completed tool prefix has no pinned schema".into()))?;
    let result = load_json::<ToolResult>(journal, result).await?;
    let projection = load_json::<Value>(journal, projection).await?;
    validate_value(&definition.output_schema, &result.value, "tool output")?;
    definition.validate_projection(&projection)?;
    Ok(projection)
}

pub(crate) async fn stage_json<T: Serialize>(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    key: &str,
    value: &T,
) -> Result<FileRef> {
    stage_bytes(
        journal,
        operation_id,
        key,
        crate::contract::canonical_json_bytes(value)?,
    )
    .await
}

async fn stage_bytes(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    key: &str,
    bytes: Vec<u8>,
) -> Result<FileRef> {
    let expected = crate::conversation::FileDescriptor::from_bytes(&bytes, "application/json")?;
    // An unpublished staged payload may survive a failed journal append. A
    // reconciled value at that position may differ; only the journal's exact
    // append claim chooses the authoritative value, not the orphaned file.
    let payload_key = format!("{key}:{}", blake3::hash(&bytes).to_hex());
    let reference = journal
        .stage(operation_id, payload_key, bytes, "application/json")
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(Error::Storage(
            "execution journal returned a non-private JSON reference".into(),
        ));
    }
    if reference.descriptor() != &expected {
        return Err(Error::Storage(
            "execution journal staged different JSON bytes".into(),
        ));
    }
    Ok(reference)
}

/// Validated retained model artifacts without provider dispatch.
pub(crate) struct RetainedModelStep {
    pub(crate) request: Option<crate::model::PreparedModelRequest>,
    pub(crate) admission: ModelEventAdmission,
    pub(crate) events: Vec<ModelEvent>,
}

/// Reads only durable artifacts; never evaluates context, policy or providers.
pub(crate) async fn retained_model_step(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    limits: Limits,
) -> Result<RetainedModelStep> {
    let mut events = Vec::new();
    let mut admission = ModelEventAdmission::default();
    for record in records {
        if let ExecutionEvent::Model {
            step: event_step,
            event,
        } = &record.event
            && *event_step == step
        {
            let event = load_json::<ModelEvent>(journal, event).await?;
            admission.observe(&event, limits)?;
            events.push(event);
        }
    }
    let started = records.iter().find_map(|record| match &record.event {
        ExecutionEvent::ModelStarted {
            step: event_step,
            request_digest,
            request,
        } if *event_step == step => Some((*request_digest, request)),
        _ => None,
    });
    if started.is_none() && !events.is_empty() {
        return Err(Error::Storage(
            "model observations exist without an admitted attempt".into(),
        ));
    }
    let request = if let Some((digest, reference)) = started {
        let recorded = load_model_request(journal, reference, limits).await?;
        if recorded.manifest().request_digest != digest {
            return Err(Error::Conflict(
                "recorded model request digest differs from admission".into(),
            ));
        }
        Some(recorded)
    } else {
        None
    };
    Ok(RetainedModelStep {
        request,
        admission,
        events,
    })
}

async fn verify_model_content_scoped(
    journal: &dyn ExecutionJournal,
    content: &ModelContent,
    limits: Limits,
    scope: Option<&RuntimeScope>,
) -> Result<()> {
    verify_model_contents_scoped(journal, std::iter::once(content), limits, scope).await
}

async fn verify_model_contents_scoped<'a>(
    journal: &dyn ExecutionJournal,
    contents: impl Iterator<Item = &'a ModelContent> + Clone,
    limits: Limits,
    scope: Option<&RuntimeScope>,
) -> Result<()> {
    for content in contents.clone() {
        validate_model_content_scope(content, limits, scope)?;
    }
    for content in contents {
        journal.verify_model_content(content).await?;
    }
    Ok(())
}

fn validate_model_content_scope(
    content: &ModelContent,
    limits: Limits,
    scope: Option<&RuntimeScope>,
) -> Result<()> {
    content.validate_limits(limits)?;
    if let Some(scope) = scope {
        content.validate_limits(scope.limits())?;
        for file in content.file_refs() {
            if !crate::runtime::read_granted(scope.grants(), file)? {
                return Err(Error::Unauthorized(
                    "attenuated task cannot read model content".into(),
                ));
            }
        }
    }
    let bindings = content.native_configurations();
    if !bindings.is_empty() {
        let scope = scope.ok_or_else(|| {
            Error::Unsupported("original native task scope is unavailable".into())
        })?;
        let schemas = scope.extension_schema_registry();
        let runtime = scope.extension_runtime();
        for binding in bindings {
            binding.verify_original(scope.extensions(), schemas.as_ref(), runtime.as_deref())?;
        }
    }
    Ok(())
}

async fn load_model_request(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
) -> Result<crate::model::PreparedModelRequest> {
    crate::model::PreparedModelRequest::decode(&load_json_bytes(journal, reference).await?, limits)
}

async fn load_json_bytes(journal: &dyn ExecutionJournal, reference: &FileRef) -> Result<Vec<u8>> {
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(Error::Storage(
            "execution journal references non-private JSON content".into(),
        ));
    }
    let bytes = journal.load(reference).await?;
    reference.descriptor().verify(&bytes)?;
    Ok(bytes)
}

pub(crate) async fn load_json<T: serde::de::DeserializeOwned>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
) -> Result<T> {
    let bytes = load_json_bytes(journal, reference).await?;
    decode_json(&bytes)
}

pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let parsed: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("execution journal JSON is invalid: {error}")))?;
    if crate::contract::canonical_json_bytes(&parsed)? != bytes {
        return Err(Error::Storage(
            "execution journal JSON is not canonical".into(),
        ));
    }
    crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("execution journal content is invalid: {error}")))
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ModelEventAdmissionState {
    #[serde(default)]
    pub(crate) count: usize,
    #[serde(default)]
    pub(crate) calls: Vec<String>,
    #[serde(default)]
    pub(crate) completed: bool,
}

#[derive(Default)]
pub(crate) struct ModelEventAdmission {
    count: usize,
    calls: BTreeSet<String>,
    completed: bool,
}

impl ModelEventAdmission {
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) fn from_state(state: ModelEventAdmissionState, limits: Limits) -> Result<Self> {
        if state.count > limits.model_events_per_step {
            return Err(Error::Invalid(
                "model event admission state exceeds its limit".into(),
            ));
        }
        let call_count = state.calls.len();
        if call_count > limits.tool_calls_per_step {
            return Err(Error::Invalid(
                "model tool call admission state exceeds its limit".into(),
            ));
        }
        let calls = state
            .calls
            .into_iter()
            .map(|call_id| {
                ToolInvocation::validate_identity(&call_id, "admitted.tool")?;
                Ok(call_id)
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if calls.len() != call_count {
            return Err(Error::Invalid(
                "model tool call admission state contains repeated identities".into(),
            ));
        }
        Ok(Self {
            count: state.count,
            calls,
            completed: state.completed,
        })
    }

    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) fn state(&self) -> ModelEventAdmissionState {
        ModelEventAdmissionState {
            count: self.count,
            calls: self.calls.iter().cloned().collect(),
            completed: self.completed,
        }
    }

    pub(crate) fn validate_next(&self, event: &ModelEvent, limits: Limits) -> Result<()> {
        if self.count >= limits.model_events_per_step {
            return Err(Error::Invalid("model event limit exceeded".into()));
        }
        if self.completed {
            return Err(Error::Invalid(
                "model emitted an event after completion".into(),
            ));
        }
        match event {
            ModelEvent::ToolCall { call_id, name, .. } => {
                ToolInvocation::validate_identity(call_id, name)?;
                if self.calls.len() >= limits.tool_calls_per_step || self.calls.contains(call_id) {
                    return Err(Error::Invalid(
                        "model tool call limit exceeded or identity repeated".into(),
                    ));
                }
            }
            ModelEvent::Completed { .. }
            | ModelEvent::Content { .. }
            | ModelEvent::Reasoning { .. } => {}
        }
        Ok(())
    }

    pub(crate) fn observe(&mut self, event: &ModelEvent, limits: Limits) -> Result<()> {
        self.validate_next(event, limits)?;
        match event {
            ModelEvent::ToolCall { call_id, .. } => {
                self.calls.insert(call_id.clone());
            }
            ModelEvent::Completed { .. } => self.completed = true,
            ModelEvent::Content { .. } | ModelEvent::Reasoning { .. } => {}
        }
        self.count += 1;
        Ok(())
    }

    pub(crate) const fn completed(&self) -> bool {
        self.completed
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, Outcome,
        conversation::{FileDescriptor, VolumeOwner, VolumeRef},
        resources::ProviderRef,
        tool::{Tool, ToolDefinition, ToolExecutor},
    };
    use futures::{FutureExt as _, stream};
    use std::collections::HashMap;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    /// Emits the slip seen in production — `parameters` where the pinned schema says `arguments`
    /// — then a well-formed call once it has been told what was wrong.
    struct SlippingModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for SlippingModel {
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            let request = request.request().clone();
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut requests) = self.requests.lock() {
                requests.push(request);
            }
            let events = match call {
                0 => vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "call-1".into(),
                        name: "example.echo".into(),
                        arguments: json!({"parameters": {"value": "hello"}}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ],
                1 => vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "call-2".into(),
                        name: "example.echo".into(),
                        arguments: json!({"value": "hello"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ],
                _ => vec![
                    Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: json!({"finish": "stop"}),
                    }),
                ],
            };
            Box::pin(stream::iter(events))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct FakeModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    async fn capture_admitted_context(scope: RuntimeScope) -> Result<crate::runtime::TaskContext> {
        let captured = Arc::new(Mutex::new(None));
        let sink = captured.clone();
        let mut tasks = crate::runtime::TaskRegistry::default();
        tasks.register(crate::runtime::TaskDefinition::live(
            "context-probe",
            "1",
            move |context, _: ()| {
                let sink = sink.clone();
                async move {
                    *sink
                        .lock()
                        .map_err(|_| Error::Storage("context lock poisoned".into()))? =
                        Some(context);
                    Ok(())
                }
            },
        )?)?;
        let harness =
            crate::runtime::AgentHarness::new(tasks, ToolRegistry::new(), scope, 1, None)?;
        let definition = harness.task::<(), ()>("context-probe@1")?;
        assert!(matches!(
            harness.spawn(&definition, ()).await?.result().await?,
            Outcome::Succeeded(())
        ));
        captured
            .lock()
            .map_err(|_| Error::Storage("context lock poisoned".into()))?
            .take()
            .ok_or_else(|| Error::NotFound("admitted context".into()))
    }

    struct ContextOnlyTool {
        admission: OperationId,
        calls: AtomicUsize,
        reconciles: AtomicUsize,
    }

    impl ToolExecutor for ContextOnlyTool {
        fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            async { Err(Error::Unsupported("original context required".into())) }.boxed()
        }

        fn execute_with_context<'a>(
            &'a self,
            context: crate::runtime::ToolContext,
            invocation: ToolInvocation,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            async move {
                assert_eq!(context.task().id(), self.admission);
                assert_eq!(context.operation_id(), invocation.operation_id);
                assert_eq!(context.call_id(), invocation.call_id);
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::Storage("response lost after effect".into()))
            }
            .boxed()
        }

        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Err(Error::Unsupported("original context required".into())) }.boxed()
        }

        fn reconcile_with_context<'a>(
            &'a self,
            context: crate::runtime::ToolContext,
            invocation: ToolInvocation,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async move {
                assert_eq!(context.task().id(), self.admission);
                assert_eq!(context.operation_id(), invocation.operation_id);
                assert_eq!(context.call_id(), invocation.call_id);
                self.reconciles.fetch_add(1, Ordering::SeqCst);
                Ok(Some(ToolResult {
                    value: json!({"value":"hello"}),
                }))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn original_context_survives_tool_response_loss_and_rejects_retargeted_replay()
    -> Result<()> {
        let scope = RuntimeScope::new(
            crate::Capabilities::new([
                "task:spawn:context-probe@1",
                "model:generate",
                "tool:call:example.echo",
            ]),
            Limits::default(),
        )?;
        let original = capture_admitted_context(scope.clone()).await?;
        let replacement = capture_admitted_context(scope.clone()).await?;
        assert_ne!(original.id(), replacement.id());
        let tool = Arc::new(ContextOnlyTool {
            admission: original.id(),
            calls: AtomicUsize::new(0),
            reconciles: AtomicUsize::new(0),
        });
        let mut tools = ToolRegistry::new();
        tools.register(Tool {
            definition: ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type":"object"})),
            },
            executor: tool.clone(),
            projection: Arc::new(Projection),
        })?;
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let base = StockExecutor::new(
            Model::new("fixture", "exact", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(scope.clone(), None)?;
        let operation = OperationId::new();
        let executor = base.clone().with_task_context(&original, operation)?;
        let input = TurnInput {
            operation_id: operation,
            input: ModelContent::Text("input".into()),
            selected_context: None,
            max_steps: 2,
        };
        let journal = Journal::default();
        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Indeterminate(_))
        ));
        let retained = journal.0.lock().unwrap().len();
        let retargeted = base.clone().with_task_context(&replacement, operation)?;
        assert!(matches!(
            retargeted.execute(input.clone(), &journal).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(journal.0.lock().unwrap().len(), retained);
        let narrowed = scope.with_run_limits(crate::runtime::TaskRunLimits {
            max_steps: Some(1),
            ..Default::default()
        })?;
        let retargeted = executor.clone().with_tool_authority(narrowed, None)?;
        let reads = journal.3.load(Ordering::SeqCst);
        assert!(matches!(
            retargeted.execute(input.clone(), &journal).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            retargeted
                .completed_tool_prefix(&journal, &input, 0, "call-1")
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(journal.0.lock().unwrap().len(), retained);
        assert_eq!(journal.3.load(Ordering::SeqCst), reads);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let result = executor.execute(input.clone(), &journal).await?;
        assert_eq!(result.text, "done");
        let reads = journal.3.load(Ordering::SeqCst);
        executor
            .completed_tool_prefix(&journal, &input, 0, "call-1")
            .await?;
        assert!(journal.3.load(Ordering::SeqCst) > reads);
        assert_eq!(executor.execute(input, &journal).await?, result);
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool.reconciles.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    impl ModelProvider for FakeModel {
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            assert_eq!(
                request.bytes(),
                crate::contract::canonical_json_bytes(request.request()).unwrap_or_default()
            );
            assert_eq!(
                request.manifest().request_digest,
                *blake3::hash(request.bytes()).as_bytes()
            );
            let request = request.request().clone();
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut requests) = self.requests.lock() {
                requests.push(request);
            }
            let events = if call == 0 {
                vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "call-1".into(),
                        name: "example.echo".into(),
                        arguments: json!({"value": "hello"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]
            } else {
                vec![
                    Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: json!({"finish": "stop"}),
                    }),
                ]
            };
            Box::pin(stream::iter(events))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct RecoverableModel {
        generate_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        dispatches: Mutex<Vec<crate::model::ModelDispatch>>,
    }

    impl ModelProvider for RecoverableModel {
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            assert_eq!(dispatch.request_digest, request.manifest().request_digest);
            let Ok(mut dispatches) = self.dispatches.lock() else {
                return Box::pin(stream::once(async {
                    Err(Error::Storage("model lock poisoned".into()))
                }));
            };
            dispatches.push(dispatch);
            self.generate_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(
                (0..70)
                    .map(|_| {
                        Ok(ModelEvent::Content {
                            delta: "partial-".into(),
                        })
                    })
                    .chain(std::iter::once(Err(Error::Storage(
                        "stream interrupted".into(),
                    )))),
            ))
        }

        fn reconcile<'a>(
            &'a self,
            attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                assert!(
                    self.dispatches
                        .lock()
                        .map_err(|_| Error::Storage("model lock poisoned".into()))?
                        .contains(&attempt.dispatch())
                );
                if attempt.observed
                    != vec![
                        ModelEvent::Content {
                            delta: "partial-".into(),
                        };
                        70
                    ]
                {
                    return Err(Error::Conflict("unexpected model event prefix".into()));
                }
                Ok(Some(vec![
                    ModelEvent::Content {
                        delta: "restored".into(),
                    },
                    ModelEvent::Completed {
                        metadata: json!({"finish": "stop"}),
                    },
                ]))
            }
            .boxed()
        }
    }

    struct FakeTool(AtomicUsize);

    impl crate::tool::ToolExecutor for FakeTool {
        fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(ToolResult {
                    value: invocation.arguments,
                })
            }
            .boxed()
        }

        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct Projection;

    impl crate::tool::ToolProjection for Projection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(serde_json::json!({"kind":"json","value":result.value}))
        }
    }

    #[derive(Default)]
    struct Journal(
        Mutex<Vec<ExecutionRecord>>,
        Mutex<HashMap<String, (FileRef, Vec<u8>)>>,
        Mutex<HashMap<InteractionId, (Interaction, Option<InteractionOutcome>)>>,
        AtomicUsize,
    );

    #[tokio::test]
    async fn execution_journal_rejects_noncanonical_json_before_replay() -> Result<()> {
        let journal = Journal::default();
        let operation = OperationId::new();
        let reference = journal
            .stage(
                operation,
                "noncanonical".into(),
                br#"{ "b":2,"a":1 }"#.to_vec(),
                "application/json",
            )
            .await?;
        assert!(matches!(load_json::<Value>(&journal, &reference).await,
            Err(Error::Storage(message)) if message.contains("not canonical")));
        Ok(())
    }

    impl ExecutionJournal for Journal {
        fn replay<'a>(
            &'a self,
            operation_id: OperationId,
            after: u64,
            maximum: u32,
        ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
            async move {
                self.3.fetch_add(1, Ordering::SeqCst);
                validate_execution_page(after, maximum)?;
                self.0
                    .lock()
                    .map(|records| {
                        records
                            .iter()
                            .filter(|record| {
                                record.operation_id == operation_id && record.sequence > after
                            })
                            .take(maximum as usize)
                            .cloned()
                            .collect()
                    })
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))
            }
            .boxed()
        }

        fn append<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: String,
            event: ExecutionEvent,
        ) -> BoxFuture<'a, Result<()>> {
            async move {
                let mut records = self
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
                if let Some(existing) = records.iter().find(|record| {
                    record.operation_id == operation_id && record.idempotency_key == idempotency_key
                }) {
                    return if existing.event == event {
                        Ok(())
                    } else {
                        Err(Error::Conflict("journal retry identity reused".into()))
                    };
                }
                let sequence = records
                    .iter()
                    .filter(|record| record.operation_id == operation_id)
                    .count() as u64
                    + 1;
                records.push(ExecutionRecord {
                    operation_id,
                    sequence,
                    idempotency_key,
                    event,
                });
                Ok(())
            }
            .boxed()
        }

        fn append_if_tail<'a>(
            &'a self,
            operation_id: OperationId,
            expected_tail: u64,
            idempotency_key: String,
            event: ExecutionEvent,
        ) -> BoxFuture<'a, Result<bool>> {
            async move {
                let mut records = self
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
                let tail = records
                    .iter()
                    .filter(|record| record.operation_id == operation_id)
                    .count() as u64;
                if tail != expected_tail {
                    return Ok(false);
                }
                records.push(ExecutionRecord {
                    operation_id,
                    sequence: tail + 1,
                    idempotency_key,
                    event,
                });
                Ok(true)
            }
            .boxed()
        }

        fn stage<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: String,
            bytes: Vec<u8>,
            media_type: &'static str,
        ) -> BoxFuture<'a, Result<FileRef>> {
            async move {
                let key = format!("{operation_id}:{idempotency_key}");
                let mut stored = self
                    .1
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
                if let Some((reference, existing)) = stored.get(&key) {
                    return if existing == &bytes {
                        Ok(reference.clone())
                    } else {
                        Err(Error::Conflict("journal staging identity reused".into()))
                    };
                }
                let volume = VolumeRef::new(
                    ProviderRef::new("test", "filesystem", "2")?,
                    "journal",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([0; 16])),
                )?;
                let reference = FileRef::new(
                    volume,
                    format!("journal/{}.json", blake3::hash(key.as_bytes()).to_hex()),
                    blake3::hash(&bytes).to_hex().to_string(),
                    FileDescriptor::from_bytes(&bytes, media_type)?,
                    "event.json",
                )?;
                stored.insert(key, (reference.clone(), bytes));
                Ok(reference)
            }
            .boxed()
        }

        fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            async move {
                self.1
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .values()
                    .find(|(stored, _)| stored == reference)
                    .map(|(_, bytes)| bytes.clone())
                    .ok_or_else(|| Error::NotFound("journal content is missing".into()))
            }
            .boxed()
        }

        fn open_interaction<'a>(
            &'a self,
            id: InteractionId,
            request: Interaction,
        ) -> BoxFuture<'a, Result<()>> {
            async move {
                request.validate()?;
                let mut interactions = self
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?;
                if let Some((original, _)) = interactions.get(&id) {
                    if original != &request {
                        return Err(Error::Conflict("approval request changed".into()));
                    }
                } else {
                    interactions.insert(id, (request, None));
                }
                Ok(())
            }
            .boxed()
        }

        fn interaction_outcome<'a>(
            &'a self,
            id: InteractionId,
        ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
            async move {
                Ok(self
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?
                    .get(&id)
                    .and_then(|(_, outcome)| outcome.clone()))
            }
            .boxed()
        }
    }

    struct PrefixJournalReader(Arc<Journal>);
    impl crate::conversation::ContentResidencyVerifier for PrefixJournalReader {
        fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let bytes = self.0.load(file).await?;
                file.descriptor().verify(&bytes)
            })
        }
        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            self.0.load(file)
        }
    }

    #[derive(Default)]
    struct PrefixBoundaryModel(Mutex<Vec<Vec<u8>>>);
    impl ModelProvider for PrefixBoundaryModel {
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.bytes().to_vec());
            Box::pin(stream::iter([Ok(ModelEvent::Completed {
                metadata: Value::Null,
            })]))
        }
        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    struct OnceStage(AtomicUsize);
    impl crate::context::ContextStage for OnceStage {
        fn name(&self) -> &str {
            "once"
        }
        fn contract(&self) -> Value {
            json!({"revision": "1"})
        }
        fn apply<'a>(
            &'a self,
            _: &'a ContextInput,
            context: crate::context::Context,
        ) -> BoxFuture<'a, Result<crate::context::Context>> {
            Box::pin(async move {
                if self.0.fetch_add(1, Ordering::SeqCst) != 0 {
                    return Err(Error::NotFound("source removed after admission".into()));
                }
                Ok(context)
            })
        }
    }

    #[tokio::test]
    async fn admitted_replay_does_not_reload_or_transform_sources() -> Result<()> {
        let stage = Arc::new(OnceStage(AtomicUsize::new(0)));
        let provider = Arc::new(PrefixBoundaryModel::default());
        let executor = StockExecutor::new(
            Model::new("test", "model", "1", Value::Null)?,
            provider.clone(),
            ContextPipeline::new([stage.clone() as Arc<dyn crate::context::ContextStage>]),
            ToolRegistry::new(),
        )
        .with_max_output_tokens(8_192)?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("exact é\0\r\n".into()),
            selected_context: None,
            max_steps: 1,
        };
        assert!(executor.model_step(&journal, &input, 1, &[]).await.is_err());
        let first = executor.model_step(&journal, &input, 0, &[]).await?;
        assert_eq!(first, executor.model_step(&journal, &input, 0, &[]).await?);
        assert_eq!(
            crate::model::PreparedModelRequest::decode(
                &provider
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("test lock".into()))?[0],
                Limits::default(),
            )?
            .request()
            .max_output_tokens,
            Some(8_192)
        );
        assert!(
            executor
                .clone()
                .with_max_output_tokens(16_384)?
                .model_step(&journal, &input, 0, &[])
                .await
                .is_err()
        );
        assert_eq!(stage.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider
                .0
                .lock()
                .map_err(|_| Error::Storage("test lock".into()))?
                .len(),
            1
        );
        // Missing admitted bytes fail closed; completed output is no substitute.
        journal
            .1
            .lock()
            .map_err(|_| Error::Storage("test lock".into()))?
            .clear();
        assert!(executor.execute(input, &journal).await.is_err());
        assert_eq!(stage.0.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn stock_prefix_reaches_provider_and_is_pinned_by_replay() -> Result<()> {
        let model = Model::new("example", "model", "1", Value::Null)?;
        let root = crate::model::PreparedModelRequest::prepare(
            ModelRequest {
                model: model.clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("parent é\0🦀".into()),
                }],
                tools: Vec::new(),
                max_output_tokens: Some(4096),
            },
            Limits::default(),
        )?;
        let journal = Arc::new(Journal::default());
        let root_file = journal
            .stage(
                OperationId::from_bytes([66; 16]),
                "root".into(),
                crate::model::ModelPrefix::select(&root, None)?.canonical_bytes()?,
                crate::model::ModelPrefix::MEDIA_TYPE,
            )
            .await?;
        let reader = Arc::new(PrefixJournalReader(journal.clone()));
        let provider = Arc::new(PrefixBoundaryModel::default());
        let executor = StockExecutor::new(
            model,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::new(),
        )
        .with_inherited_prefix(root_file, reader)?;
        let input = TurnInput {
            operation_id: OperationId::from_bytes([67; 16]),
            input: ModelContent::Text("child task; fresh scratch".into()),
            selected_context: None,
            max_steps: 1,
        };
        executor.execute(input.clone(), journal.as_ref()).await?;
        executor.execute(input.clone(), journal.as_ref()).await?;
        let mut expected = root.request().clone();
        // An inherited prefix carries history, while the child chooses its budget.
        expected.max_output_tokens = None;
        expected.messages.push(ModelMessage {
            role: ModelRole::User,
            content: input.input.clone(),
        });
        let expected = crate::model::PreparedModelRequest::prepare(expected, Limits::default())?;
        assert_eq!(
            *provider
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![expected.bytes().to_vec()]
        );
        let unbound = StockExecutor::new(
            root.request().model.clone(),
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::new(),
        );
        assert!(matches!(
            unbound.execute(input, journal.as_ref()).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stock_limits_bound_admission_and_pin_replay() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            },
            executor: Arc::new(FakeTool(AtomicUsize::new(0))),
            projection: Arc::new(Projection),
        })?;
        let base = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([14; 16]),
            input: ModelContent::Text("bounded".into()),
            selected_context: None,
            max_steps: 2,
        };
        let narrow = Limits {
            model_steps: 1,
            ..Limits::default()
        };
        assert!(matches!(
            base.clone()
                .with_limits(narrow)
                .execute(input.clone(), &journal)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        let _ = base.execute(input.clone(), &journal).await?;
        assert!(
            model
                .requests
                .lock()
                .map_err(|_| Error::Storage("test lock".into()))?
                .iter()
                .all(|request| request.max_output_tokens.is_none())
        );
        let changed = Limits {
            model_steps: 3,
            ..Limits::default()
        };
        assert!(matches!(
            base.with_limits(changed).execute(input, &journal).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stock_approval_wait_distinguishes_pending_and_known_rejection() -> Result<()> {
        struct ApprovalPolicy;
        impl ToolPolicy for ApprovalPolicy {
            fn identity(&self) -> ComponentIdentity {
                ComponentIdentity {
                    name: "test.approval".into(),
                    version: "1".into(),
                    digest: [7; 32],
                }
            }
            fn evaluate<'a>(
                &'a self,
                _: &'a ToolInvocation,
                _: &'a RuntimeScope,
            ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
                Box::pin(async {
                    Ok(ToolPolicyDecision::RequireApproval {
                        prompt: "Approve echo".into(),
                    })
                })
            }
        }
        for outcome in [InteractionOutcome::Approved, InteractionOutcome::Denied] {
            let model = Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            });
            let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
            let mut tools = ToolRegistry::new();
            tools.register(crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type": "object"}),
                    output_schema: json!({"type": "object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type": "object"}),
                    ),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            })?;
            let executor = StockExecutor::new(
                Model::new("example", "model", "1", Value::Null)?,
                model.clone(),
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["tool:call:example.echo", capability::INTERACTION_ROUTE]),
                    Limits::default(),
                )?,
                Some(Arc::new(ApprovalPolicy)),
            )?;
            let journal = Journal::default();
            let input = TurnInput {
                operation_id: OperationId::from_bytes([28; 16]),
                input: ModelContent::Text("hello".into()),
                selected_context: None,
                max_steps: 4,
            };
            let invocation = ToolInvocation::for_model_call(
                input.operation_id,
                0,
                "call-1".into(),
                "example.echo".into(),
                json!({"value":"hello"}),
            );
            for _ in 0..2 {
                assert!(
                    matches!(executor.execute_progress(input.clone(), &journal).await?, StockTurnProgress::Pending(operation) if operation == invocation.operation_id)
                );
            }
            assert!(
                matches!(executor.execute(input.clone(), &journal).await, Err(Error::Indeterminate(operation)) if operation == invocation.operation_id)
            );
            assert_eq!(model.calls.load(Ordering::SeqCst), 1);
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
            let (id, request) = {
                let interactions = journal
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?;
                assert_eq!(interactions.len(), 1);
                interactions
                    .iter()
                    .next()
                    .map(|(id, (request, _))| (*id, request.clone()))
                    .ok_or_else(|| Error::NotFound("approval".into()))?
            };
            assert!(
                matches!(request, Interaction::Approval { operation_id, .. } if operation_id == invocation.operation_id)
            );
            journal
                .2
                .lock()
                .map_err(|_| Error::Storage("interaction lock".into()))?
                .get_mut(&id)
                .ok_or_else(|| Error::NotFound("approval".into()))?
                .1 = Some(outcome.clone());
            if outcome == InteractionOutcome::Approved {
                assert!(matches!(
                    executor.execute_progress(input.clone(), &journal).await?,
                    StockTurnProgress::Ready(TurnOutput { steps: 2, .. })
                ));
                assert_eq!(model.calls.load(Ordering::SeqCst), 2);
                assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
                executor.execute(input, &journal).await?;
                assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(
                    executor.execute_progress(input.clone(), &journal).await?,
                    StockTurnProgress::Rejected(crate::InteractionRejection::Denied)
                ));
                assert_eq!(model.calls.load(Ordering::SeqCst), 1);
                assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
                // A retained in-flight tool is uncertainty, even if approval is
                // subsequently rejected. It cannot become a passive wait or a
                // terminal result without provider settlement.
                let reference =
                    stage_json(&journal, input.operation_id, "tool-start", &invocation).await?;
                journal
                    .append(
                        input.operation_id,
                        "tool:0:call-1:started".into(),
                        ExecutionEvent::ToolStarted {
                            step: 0,
                            call_id: invocation.call_id.clone(),
                            invocation: reference,
                        },
                    )
                    .await?;
                assert!(
                    matches!(executor.execute_progress(input, &journal).await, Err(Error::Indeterminate(operation)) if operation == invocation.operation_id)
                );
                assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn stock_loop_replays_without_reinvoking_models_or_tools() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            },
            executor: tool_executor.clone(),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([1; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        assert!(
            executor
                .completed_tool_prefix(&journal, &input, 0, "call-1")
                .await
                .is_err()
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        let first = executor.execute(input.clone(), &journal).await?;
        let completed_prefix = executor
            .completed_tool_prefix(&journal, &input, 0, "call-1")
            .await?;
        let dispatched = model
            .requests
            .lock()
            .map_err(|_| Error::Storage("model lock poisoned".into()))?
            .get(1)
            .cloned()
            .ok_or_else(|| Error::NotFound("second model input".into()))?;
        assert_eq!(
            completed_prefix.bytes(),
            crate::model::PreparedModelRequest::prepare(dispatched, Limits::default())?.bytes()
        );
        assert!(matches!(
            executor
                .completed_tool_prefix(&journal, &input, 0, "missing-call")
                .await,
            Err(Error::Indeterminate(_))
        ));

        let mut replay_context = Vec::new();
        let changed_call = ToolInvocation::for_model_call(
            input.operation_id,
            0,
            "call-1".into(),
            "example.echo".into(),
            json!({"value": "changed"}),
        );
        assert!(matches!(
            executor
                .resolve_tool_call(
                    &journal,
                    input.operation_id,
                    0,
                    changed_call,
                    &mut replay_context,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(replay_context.is_empty());
        let replayed = executor.execute(input.clone(), &journal).await?;
        assert_eq!(first, replayed);
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        let reopened = Journal(
            Mutex::new(
                journal
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .clone(),
            ),
            Mutex::new(
                journal
                    .1
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .clone(),
            ),
            Mutex::new(
                journal
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?
                    .clone(),
            ),
            AtomicUsize::new(0),
        );
        assert_eq!(executor.execute(input.clone(), &reopened).await?, first);
        let incomplete = Journal(
            Mutex::new(
                reopened
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .iter()
                    .take_while(|record| {
                        !matches!(record.event, ExecutionEvent::ToolCompleted { .. })
                    })
                    .cloned()
                    .collect(),
            ),
            Mutex::new(
                reopened
                    .1
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .clone(),
            ),
            Mutex::new(
                reopened
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?
                    .clone(),
            ),
            AtomicUsize::new(0),
        );
        assert!(matches!(
            executor
                .completed_tool_prefix(&incomplete, &input, 0, "call-1")
                .await,
            Err(Error::Indeterminate(_))
        ));
        let gapped = Journal(
            Mutex::new(
                incomplete
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .iter()
                    .filter(|record| record.sequence != 2)
                    .cloned()
                    .collect(),
            ),
            Mutex::new(
                incomplete
                    .1
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                    .clone(),
            ),
            Mutex::new(
                incomplete
                    .2
                    .lock()
                    .map_err(|_| Error::Storage("interaction lock".into()))?
                    .clone(),
            ),
            AtomicUsize::new(0),
        );
        assert!(matches!(
            executor
                .completed_tool_prefix(&gapped, &input, 0, "call-1")
                .await,
            Err(Error::Conflict(_))
        ));
        let request_ref = reopened
            .0
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .iter()
            .find_map(|record| match &record.event {
                ExecutionEvent::ModelStarted {
                    step: 0, request, ..
                } => Some(request.clone()),
                _ => None,
            })
            .ok_or_else(|| Error::NotFound("recorded model start".into()))?;
        let request_key = reopened
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .iter()
            .find_map(|(key, (reference, _))| (reference == &request_ref).then(|| key.clone()))
            .ok_or_else(|| Error::NotFound("recorded request".into()))?;
        reopened
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .get_mut(&request_key)
            .ok_or_else(|| Error::NotFound("recorded request".into()))?
            .1 = b"{}".to_vec();
        assert!(executor.execute(input.clone(), &reopened).await.is_err());
        reopened
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .remove(&request_key);
        assert!(matches!(
            executor.execute(input, &reopened).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        let durable = serde_json::to_string(
            &*journal
                .0
                .lock()
                .map_err(|_| Error::Storage("journal lock poisoned".into()))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(
            !durable.contains("hello")
                && !durable.contains("done")
                && !durable.contains("partial-")
        );
        assert!(
            !journal
                .1
                .lock()
                .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                .is_empty()
        );
        let requests = model
            .requests
            .lock()
            .map_err(|_| Error::Storage("model lock poisoned".into()))?;
        assert_eq!(
            requests.get(1).map(|request| request
                .messages
                .iter()
                .map(|message| message.role.as_str())
                .collect::<Vec<_>>()),
            Some(vec!["user", "assistant", "tool"])
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn stock_turn_emits_spans_without_prompt_or_tool_fields() -> Result<()> {
        use crate::obs::capture;
        let (seen, _guard) = capture::install();
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            },
            executor: Arc::new(FakeTool(AtomicUsize::new(0))),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([1; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        executor.execute(input.clone(), &journal).await?;
        executor.execute(input, &journal).await?;

        let has = |span, field, value| capture::has(&seen, span, field, value);
        assert!(has("acyclic.harness.executor.execute", "outcome", "ok"));
        let step = "acyclic.harness.executor.model_step";
        assert!(has(step, "model", "model"));
        assert!(has(step, "step", "1"));
        assert!(has(step, "phase", "dispatch"));
        assert!(has(step, "phase", "replay"));
        assert!(has(step, "items", "2"));
        assert!(has(step, "outcome", "ok"));
        assert!(has("acyclic.harness.executor.tool_call", "step", "0"));
        assert!(has("acyclic.harness.executor.tool_call", "outcome", "ok"));
        capture::assert_clean(&seen, &["hello", "done", "example.echo"]);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_stock_hosts_do_not_redispatch_model_or_tool() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type":"object"})),
            },
            executor: tool_executor.clone(),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([77; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        let (left, right) = tokio::join!(
            executor.execute(input.clone(), &journal),
            executor.execute(input.clone(), &journal)
        );
        assert!(
            left.is_ok()
                || right.is_ok()
                || matches!(left, Err(Error::Indeterminate(_)))
                    && matches!(right, Err(Error::Indeterminate(_)))
        );
        let _ = executor.execute(input, &journal).await?;
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_tool_arguments_fail_closed_before_dispatch() -> Result<()> {
        let model = Arc::new(SlippingModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "additionalProperties": false,
                }),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            },
            executor: tool_executor.clone(),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([79; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };

        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_ungranted_tool_is_refused_before_its_schema_can_be_probed() -> Result<()> {
        // Malformed arguments against a registered but ungranted tool must not be answered with a
        // validation message: that message describes the tool's pinned input schema, so answering
        // it would let a model map the contract of a tool it may not call.
        let model = Arc::new(SlippingModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "additionalProperties": false,
                }),
                output_schema: json!({"type": "object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
            },
            executor: tool_executor.clone(),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model,
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            // Registered, but this scope grants a different tool.
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.other"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([80; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        let outcome = executor.execute(input, &journal).await;
        assert!(
            matches!(&outcome, Err(Error::Unauthorized(message)) if message.contains("example.echo")),
            "expected an unauthorized refusal, got {outcome:?}"
        );
        // Nothing about the input contract leaked on the way out.
        let rendered = format!("{outcome:?}");
        assert!(
            !rendered.contains("additionalProperties") && !rendered.contains("failed validation"),
            "schema detail leaked through the refusal: {rendered}"
        );
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn completed_tool_prefix_validates_retained_result_and_projection_independently()
    -> Result<()> {
        let definition = crate::tool::ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Structured result, text projection".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            projection_schema: crate::tool::json_projection_schema(json!({"type": "string"})),
        };
        // Each retained payload has a valid digest and canonical encoding.
        // Failures below concern the independently admitted schemas.
        for (canonical, projected, accepted) in [
            (
                json!({"count": 2}),
                json!({"kind":"json","value":"two"}),
                true,
            ),
            (
                json!("bad result"),
                json!({"kind":"json","value":"two"}),
                false,
            ),
            (
                json!({"count": 2}),
                json!({"kind":"json","value":{"bad":"projection"}}),
                false,
            ),
        ] {
            let journal = Journal::default();
            let operation = OperationId::new();
            let invocation = ToolInvocation::for_model_call(
                operation,
                0,
                "call".into(),
                definition.name.clone(),
                json!({}),
            );
            let invocation_ref = stage_json(&journal, operation, "invocation", &invocation).await?;
            let result = stage_json(
                &journal,
                operation,
                "result",
                &ToolResult { value: canonical },
            )
            .await?;
            let projection = stage_json(&journal, operation, "projection", &projected).await?;
            journal
                .append(
                    operation,
                    "started".into(),
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: invocation.call_id.clone(),
                        invocation: invocation_ref,
                    },
                )
                .await?;
            journal
                .append(
                    operation,
                    "completed".into(),
                    ExecutionEvent::ToolCompleted {
                        step: 0,
                        call_id: invocation.call_id.clone(),
                        result,
                        projection,
                    },
                )
                .await?;
            let records = journal.replay(operation, 0, 64).await?;
            let retained = completed_tool_projection(
                &journal,
                &records,
                0,
                &invocation,
                std::slice::from_ref(&definition),
            )
            .await;
            if accepted {
                assert_eq!(retained?, projected);
            } else {
                assert!(retained.is_err());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn stock_tool_validation_failure_replays_as_terminal_without_redispatch() -> Result<()> {
        // The executor returns an object. Each case invalidates exactly one
        // contract, and replay must retain that terminal failure.
        for (output_schema, projection_schema, expected) in [
            (
                json!({"type":"string"}),
                json!({"type":"object"}),
                ToolFailureKind::InvalidOutput,
            ),
            (
                json!({"type":"object"}),
                json!({"type":"string"}),
                ToolFailureKind::ProjectionRejected,
            ),
        ] {
            let model = Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            });
            let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
            let mut tools = ToolRegistry::new();
            tools.register(crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema,
                    projection_schema: crate::tool::json_projection_schema(projection_schema),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            })?;
            let executor = StockExecutor::new(
                Model::new("example", "model", "1", Value::Null)?,
                model,
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["tool:call:example.echo"]),
                    Limits::default(),
                )?,
                None,
            )?;
            let journal = Journal::default();
            let input = TurnInput {
                operation_id: OperationId::from_bytes([78; 16]),
                input: ModelContent::Text("hello".into()),
                selected_context: None,
                max_steps: 4,
            };
            assert!(matches!(executor.execute(input.clone(), &journal).await,
            Err(Error::Invalid(message)) if message == expected.message()));
            assert!(matches!(executor.execute(input, &journal).await,
            Err(Error::Invalid(message)) if message == expected.message()));
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn operation_identity_rejects_changed_input() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model,
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let operation_id = OperationId::from_bytes([9; 16]);
        let _ = executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Text("first".into()),
                    selected_context: None,
                    max_steps: 1,
                },
                &journal,
            )
            .await;
        assert!(matches!(
            executor
                .execute(
                    TurnInput {
                        operation_id,
                        input: ModelContent::Text("changed".into()),
                        selected_context: None,
                        max_steps: 1,
                    },
                    &journal,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_model_stream_reconciles_without_redispatch() -> Result<()> {
        struct DispatchObserver {
            claims: AtomicUsize,
            owned: std::sync::atomic::AtomicBool,
        }
        impl DurableTaskHost for DispatchObserver {
            fn verify_execution_owner<'a>(
                &'a self,
                _: TaskId,
                _: crate::scheduler::LeaseFence,
            ) -> BoxFuture<'a, Result<()>> {
                Box::pin(async move {
                    if self.owned.load(Ordering::SeqCst) {
                        Ok(())
                    } else {
                        Err(Error::Conflict("stale test owner".into()))
                    }
                })
            }
            fn claim_model_dispatch<'a>(
                &'a self,
                _: TaskId,
                _: OperationId,
                _: u32,
                _: [u8; 32],
                _: crate::scheduler::LeaseFence,
            ) -> BoxFuture<'a, Result<()>> {
                Box::pin(async move {
                    self.claims.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }
            fn outcome<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
                Box::pin(async { Err(Error::Unsupported("test observer".into())) })
            }
            fn cancel<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<()>> {
                Box::pin(async { Err(Error::Unsupported("test observer".into())) })
            }
        }
        // This observer checks executor calls, not budget semantics. The real
        // CoordinatorTaskHost test covers the authoritative pinned ceiling.
        let host = Arc::new(DispatchObserver {
            claims: AtomicUsize::new(0),
            owned: std::sync::atomic::AtomicBool::new(true),
        });
        let model = Arc::new(RecoverableModel {
            generate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            dispatches: Mutex::new(Vec::new()),
        });
        let executor = StockExecutor::new(
            Model::new("example", "recoverable", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_durable_task(
            host.clone(),
            TaskId::from_bytes([11; 16]),
            crate::scheduler::LeaseFence {
                reservation_id: "lease".into(),
                placement: "worker".into(),
            },
        );
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([10; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 1,
        };

        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Storage(_))
        ));
        let recovered = executor.execute(input.clone(), &journal).await?;
        let replayed = executor.execute(input.clone(), &journal).await?;

        assert_eq!(recovered.text, format!("{}restored", "partial-".repeat(70)));
        assert_eq!(replayed, recovered);
        assert_eq!(model.generate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.claims.load(Ordering::SeqCst), 1);
        assert_eq!(journal.replay(input.operation_id, 0, 64).await?.len(), 64);
        assert_eq!(journal.replay(input.operation_id, 64, 64).await?.len(), 10);
        assert!(journal.replay(input.operation_id, 74, 64).await?.is_empty());
        host.owned.store(false, Ordering::SeqCst);
        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(model.generate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.reconcile_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn execution_pages_select_bounded_records_and_reject_cross_page_corruption() -> Result<()>
    {
        let journal = Journal::default();
        let operation = OperationId::from_bytes([29; 16]);
        for step in 0..70 {
            journal
                .append(
                    operation,
                    format!("step-{step}"),
                    ExecutionEvent::ModelStarted {
                        step,
                        request_digest: [1; 32],
                        request: journal
                            .stage(
                                operation,
                                "page-fixture".into(),
                                b"null".to_vec(),
                                "application/json",
                            )
                            .await?,
                    },
                )
                .await?;
        }
        let (tail, selected) = replay_execution(&journal, operation, 1, |event| {
            matches!(event, ExecutionEvent::ModelStarted { step: 69, .. })
        })
        .await?;
        assert_eq!(tail, 70);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].sequence, 70);
        assert_eq!(journal.replay(operation, 0, 64).await?.len(), 64);
        assert_eq!(journal.replay(operation, 64, 64).await?.len(), 6);
        assert_eq!(journal.replay(operation, 0, 65).await?.len(), 65);
        assert!(journal.replay(operation, 0, 0).await.is_err());
        assert!(
            replay_execution(&journal, operation, 1, |_| true)
                .await
                .is_err()
        );
        let original = {
            let mut records = journal
                .0
                .lock()
                .map_err(|_| Error::Storage("journal lock".into()))?;
            let original = records[69].idempotency_key.clone();
            records[69].idempotency_key = records[0].idempotency_key.clone();
            original
        };
        assert!(matches!(
            replay_execution(&journal, operation, 0, |_| false).await,
            Err(Error::Conflict(_))
        ));
        let mut cursor = ExecutionReplay::new(operation);
        assert_eq!(
            cursor.next_page(&journal).await?.map(|page| page.len()),
            Some(64)
        );
        for _ in 0..2 {
            assert!(matches!(
                cursor.next_page(&journal).await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(
                cursor.tail(),
                64,
                "a corrupt page must not advance partially"
            );
        }
        {
            let mut records = journal
                .0
                .lock()
                .map_err(|_| Error::Storage("journal lock".into()))?;
            records[69].idempotency_key = original;
            records[69].sequence += 1;
        }
        assert!(matches!(
            replay_execution(&journal, operation, 0, |_| false).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn prefix_replay_accepts_platform_maximum_counts() -> Result<()> {
        let mut executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            ContextPipeline::default(),
            ToolRegistry::new(),
        );
        // Exercise the address-width boundary on every platform. On WASM32,
        // these are the default counts, even when the retained history is tiny.
        executor.limits.model_events_per_step = usize::MAX;
        executor.limits.tool_calls_per_step = usize::MAX;
        let journal = Journal::default();
        let operation = OperationId::from_bytes([27; 16]);
        journal
            .append(
                operation,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
            )
            .await?;
        let records = executor.prefix_records(&journal, operation, 0).await?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence, 1);
        Ok(())
    }

    #[test]
    fn independently_selected_event_budget_still_rejects_excess_observations() -> Result<()> {
        let limits = Limits {
            model_events_per_step: 1,
            ..Limits::default()
        };
        limits.validate()?;
        let mut admission = ModelEventAdmission::default();
        let event = ModelEvent::ToolCall {
            call_id: "call-1".into(),
            name: "example.echo".into(),
            arguments: Value::Null,
        };
        admission.observe(&event, limits)?;
        assert!(matches!(
            admission.observe(&ModelEvent::Completed { metadata: Value::Null }, limits),
            Err(Error::Invalid(reason)) if reason == "model event limit exceeded"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn journal_retry_keys_and_sequences_are_operation_scoped() -> Result<()> {
        let journal = Journal::default();
        let first = OperationId::from_bytes([20; 16]);
        let second = OperationId::from_bytes([21; 16]);
        for (operation_id, digest) in [(first, [1; 32]), (second, [2; 32])] {
            journal
                .append(
                    operation_id,
                    "execution:started".into(),
                    ExecutionEvent::Started {
                        request_digest: digest,
                    },
                )
                .await?;
        }
        let replayed_first = journal
            .replay(first, 0, EXECUTION_REPLAY_PAGE_RECORDS)
            .await?;
        let [first_entry] = replayed_first.as_slice() else {
            unreachable!("expected exactly one replayed event for the first operation");
        };
        assert_eq!(first_entry.sequence, 1);
        let replayed_second = journal
            .replay(second, 0, EXECUTION_REPLAY_PAGE_RECORDS)
            .await?;
        let [second_entry] = replayed_second.as_slice() else {
            unreachable!("expected exactly one replayed event for the second operation");
        };
        assert_eq!(second_entry.sequence, 1);
        Ok(())
    }
    #[tokio::test]
    async fn identical_model_bytes_keep_distinct_dispatch_identities() -> Result<()> {
        #[derive(Default)]
        struct Provider(Mutex<Vec<(crate::model::ModelDispatch, Vec<u8>)>>);
        impl ModelProvider for Provider {
            fn generate<'a>(
                &'a self,
                request: crate::model::PreparedModelRequest,
                dispatch: crate::model::ModelDispatch,
            ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
                Box::pin(stream::once(async move {
                    assert_eq!(dispatch.request_digest, request.manifest().request_digest);
                    self.0
                        .lock()
                        .map_err(|_| Error::Storage("capture lock poisoned".into()))?
                        .push((dispatch, request.bytes().to_vec()));
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    })
                }))
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Err(Error::Unsupported("completed attempts must replay".into())) })
            }
        }
        let provider = Arc::new(Provider::default());
        let executor = StockExecutor::new(
            Model::new("fixture", "identity", "1", Value::Null)?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let operations = [
            OperationId::from_bytes([31; 16]),
            OperationId::from_bytes([32; 16]),
        ];
        let input = |operation_id| TurnInput {
            operation_id,
            input: ModelContent::Text("same input".into()),
            selected_context: None,
            max_steps: 1,
        };
        for operation in operations {
            executor.execute(input(operation), &journal).await?;
        }
        executor.execute(input(operations[0]), &journal).await?;
        let captures = provider
            .0
            .lock()
            .map_err(|_| Error::Storage("capture lock poisoned".into()))?;
        assert_eq!(captures.len(), 2);
        assert_eq!(captures[0].0.operation_id, operations[0]);
        assert_eq!(captures[1].0.operation_id, operations[1]);
        assert_eq!(captures[0].0.step, 0);
        assert_eq!(captures[1].0.step, 0);
        assert_eq!(captures[0].0.request_digest, captures[1].0.request_digest);
        assert_eq!(captures[0].1, captures[1].1);
        Ok(())
    }
    #[tokio::test]
    async fn native_result_reads_obey_effective_limits_and_attenuated_grants() -> Result<()> {
        let journal = ModelReadSpy::default();
        let file = journal
            .stage(
                OperationId::new(),
                "media".into(),
                vec![1, 2, 3, 4],
                "image/png",
            )
            .await?;
        let options = journal
            .stage(
                OperationId::new(),
                "options".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        let content = ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "media".into(),
            name: "test.media".into(),
            content: crate::model::ToolResultContent::Parts {
                parts: vec![crate::model::ModelDataPart::File {
                    file: file.clone(),
                    policy: crate::model::FileProjectionPolicy::Native(Box::new(
                        crate::model::NativeMediaPolicy {
                            intent: crate::model::NativeMediaIntent::Image {
                                detail: crate::model::ImageDetail::Auto,
                            },
                            maximum_bytes: 4,
                            maximum_work: 4,
                            configuration: Some(crate::model::NativeConfigurationBinding {
                                source: crate::core::EventReference {
                                    authority: crate::core::Authority {
                                        kind: crate::core::AggregateKind::Agent,
                                        id: "test-agent".into(),
                                    },
                                    revision: 1,
                                },
                                configuration: crate::core::ExtensionConfiguration {
                                    extension: crate::core::ExtensionDependency {
                                        name: "test.media".into(),
                                        version: 1,
                                    },
                                    schema_digest: [1; 32],
                                    content: options.clone(),
                                },
                                implementation_digest: [2; 32],
                            }),
                        },
                    )),
                }],
            },
        });
        let denied = RuntimeScope::new(crate::Capabilities::default(), Limits::default())?;
        assert!(matches!(
            verify_model_content_scoped(&journal, &content, Limits::default(), Some(&denied)).await,
            Err(Error::Unauthorized(_))
        ));
        let media_only = RuntimeScope::new(
            crate::Capabilities::new([file.read_capability()?]),
            Limits::default(),
        )?;
        assert!(matches!(
            verify_model_content_scoped(&journal, &content, Limits::default(), Some(&media_only))
                .await,
            Err(Error::Unauthorized(_))
        ));
        let allowed = RuntimeScope::new(
            crate::Capabilities::new([file.read_capability()?, options.read_capability()?]),
            Limits::default(),
        )?;
        assert!(matches!(
            verify_model_content_scoped(
                &journal,
                &content,
                Limits {
                    file_bytes: 3,
                    ..Limits::default()
                },
                Some(&allowed)
            )
            .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(journal.reads.load(Ordering::SeqCst), 0);
        // Read grants alone cannot authenticate a claimed native configuration.
        assert!(matches!(
            verify_model_contents_scoped(
                &journal,
                [
                    ModelContent::Part(ModelContentPart::File {
                        file: file.clone(),
                        policy: crate::model::FileProjectionPolicy::Reference
                    }),
                    content
                ]
                .iter(),
                Limits::default(),
                Some(&allowed)
            )
            .await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(journal.reads.load(Ordering::SeqCst), 0);
        let references = ModelContent::Parts(
            [file, options]
                .map(|file| ModelContentPart::File {
                    file,
                    policy: crate::model::FileProjectionPolicy::Reference,
                })
                .to_vec(),
        );
        verify_model_content_scoped(&journal, &references, Limits::default(), Some(&allowed))
            .await?;
        assert_eq!(journal.reads.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn stock_tool_input_obeys_narrow_executor_bound_before_journal_io() -> Result<()> {
        let wide = RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?;
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "bounded".into(),
            name: "example.echo".into(),
            arguments: json!({"text":"x".repeat(256)}),
        };
        let adapter = Arc::new(FakeTool(AtomicUsize::new(0)));
        // This generic adapter accepts the original scope without a TaskContext.
        // The SDK's effective byte bound must independently stop journal access.
        crate::tool::ToolExecutor::authorize(adapter.as_ref(), Some(&wide), &invocation)?;
        crate::contract::validate_json_byte_bound(&invocation, wide.limits().file_bytes)?;
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type":"object"})),
            },
            executor: adapter.clone(),
            projection: Arc::new(Projection),
        })?;
        let narrow = Limits {
            file_bytes: 128,
            ..wide.limits()
        };
        let executor = StockExecutor::new(
            Model::new("test", "scoped", "1", Value::Null)?,
            Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(wide, None)?
        .with_limits(narrow);
        assert!(executor.tool_scope.limits().file_bytes > executor.limits.file_bytes);
        let journal = ModelReadSpy::default();
        let mut messages = Vec::new();
        assert!(matches!(
            executor
                .settle_tool_call(&journal, OperationId::new(), 0, invocation, &mut messages)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(adapter.0.load(Ordering::SeqCst), 0);
        assert_eq!(journal.replays.load(Ordering::SeqCst), 0);
        assert_eq!(journal.reads.load(Ordering::SeqCst), 0);
        assert!(journal.journal.0.lock().unwrap().is_empty());
        assert!(journal.journal.1.lock().unwrap().is_empty());
        assert!(messages.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn later_model_references_fail_preflight_before_earlier_content_reads() -> Result<()> {
        let journal = ModelReadSpy::default();
        let first = journal
            .stage(OperationId::new(), "first".into(), vec![1, 2], "image/png")
            .await?;
        let second = journal
            .stage(OperationId::new(), "second".into(), vec![3; 8], "image/png")
            .await?;
        let contents = [first.clone(), second.clone()].map(|file| {
            ModelContent::Part(ModelContentPart::File {
                file,
                policy: crate::model::FileProjectionPolicy::Reference,
            })
        });
        let first_only = RuntimeScope::new(
            Capabilities::new([first.read_capability()?]),
            Limits::default(),
        )?;
        assert!(matches!(
            verify_model_contents_scoped(
                &journal,
                contents.iter(),
                Limits::default(),
                Some(&first_only)
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(journal.reads.load(Ordering::SeqCst), 0);
        let grants = Capabilities::new([first.read_capability()?, second.read_capability()?]);
        let narrow = RuntimeScope::new(
            grants.clone(),
            Limits {
                file_bytes: 4,
                ..Limits::default()
            },
        )?;
        assert!(matches!(
            verify_model_contents_scoped(
                &journal,
                contents.iter(),
                Limits::default(),
                Some(&narrow)
            )
            .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(journal.reads.load(Ordering::SeqCst), 0);
        let allowed = RuntimeScope::new(grants, Limits::default())?;
        verify_model_contents_scoped(&journal, contents.iter(), Limits::default(), Some(&allowed))
            .await?;
        assert_eq!(journal.reads.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[derive(Default)]
    struct ModelReadSpy {
        journal: Journal,
        reads: AtomicUsize,
        replays: AtomicUsize,
    }

    impl ExecutionJournal for ModelReadSpy {
        fn replay<'a>(
            &'a self,
            operation: OperationId,
            after: u64,
            maximum: u32,
        ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
            self.replays.fetch_add(1, Ordering::SeqCst);
            self.journal.replay(operation, after, maximum)
        }
        fn append<'a>(
            &'a self,
            operation: OperationId,
            key: String,
            event: ExecutionEvent,
        ) -> BoxFuture<'a, Result<()>> {
            self.journal.append(operation, key, event)
        }
        fn stage<'a>(
            &'a self,
            operation: OperationId,
            key: String,
            bytes: Vec<u8>,
            media: &'static str,
        ) -> BoxFuture<'a, Result<FileRef>> {
            self.journal.stage(operation, key, bytes, media)
        }
        fn load<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.journal.load(file)
        }
        fn open_interaction<'a>(
            &'a self,
            id: InteractionId,
            interaction: Interaction,
        ) -> BoxFuture<'a, Result<()>> {
            self.journal.open_interaction(id, interaction)
        }
        fn interaction_outcome<'a>(
            &'a self,
            id: InteractionId,
        ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
            self.journal.interaction_outcome(id)
        }
        // This spy exercises the real pre-reader scope barrier, not option authentication.
        fn verify_model_content<'a>(
            &'a self,
            content: &'a ModelContent,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                for file in content.file_refs() {
                    file.descriptor().verify(&self.load(file).await?)?;
                }
                Ok(())
            })
        }
    }
}
