//! Fully replaceable turn execution and the stock streaming model/tool loop.

use crate::{
    Error, InteractionId, OperationId, Result, TaskId,
    batch_publication::{ModelBatchPublication, ModelBatchPublisher},
    context::{ContextInput, ContextPipeline},
    conversation::{Attachment, FileRef, Limits, VolumeClass},
    core::EffectGuarantee,
    interaction::{Interaction, InteractionOutcome},
    model::{
        Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
        ModelProvider, ModelRequest, ModelRole,
    },
    projection::SelectedModelContext,
    registry::ComponentIdentity,
    runtime::{
        RuntimeScope, ToolPolicy, ToolPolicyDecision, check_tool_approval, validate_policy_identity,
    },
    tool::{
        ModelToolContext, ToolInvocation, ToolRegistry, ToolRejectionFeedback, ToolResult,
        validate_value,
    },
};
use futures::{StreamExt as _, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

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
#[serde(deny_unknown_fields)]
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

/// Semantic version of the durable completed-tool event. A missing field is
/// decoded as zero so old journals can be rejected with a typed conflict
/// instead of surfacing a generic deserialization failure.
pub const TOOL_COMPLETED_EVENT_VERSION: u32 = 2;

/// Canonical executor observation suitable for a durable journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
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
    /// Exact model-input manifest committed before provider dispatch.
    ModelInputPrepared {
        /// Zero-based executor step.
        step: u32,
        /// Private immutable file containing the ordered input manifest.
        manifest: FileRef,
        /// Immutable canonical request actually supplied to the provider.
        request: FileRef,
    },
    /// All tool results in an ordered batch are durable before children may run.
    ToolBatchCompleted {
        /// Zero-based executor step.
        step: u32,
        /// Exact completed request and its frozen inherited prefix.
        boundary: FileRef,
    },

    /// Publication admission persisted before child activation.
    BatchPublicationStarted {
        /// Zero-based executor step.
        step: u32,
        /// Complete immutable publication request.
        publication: FileRef,
    },
    /// Publication outcome persisted before a later parent model request.
    BatchPublicationCompleted {
        /// Zero-based executor step.
        step: u32,
        /// Identity of the exact admitted publication.
        publication_digest: [u8; 32],
    },
    /// A model request identity committed before provider dispatch.
    ModelStarted {
        /// Zero-based executor step.
        step: u32,
        /// Digest of the exact model request.
        request_digest: [u8; 32],
    },
    /// One model stream item was observed.
    Model {
        /// Zero-based executor step.
        step: u32,
        /// Pinned, private JSON file containing one observed model event.
        event: FileRef,
    },
    /// A call was refused before admission; no executor effect was dispatched.
    ToolAdmissionRejected {
        /// Zero-based executor step.
        step: u32,
        /// Immutable rejected model invocation.
        invocation: FileRef,
        /// Stable reason without provider exception text or credentials.
        reason: ToolRejectionKind,
        /// Optional pinned model-visible feedback for recoverable argument
        /// rejection. This reference binds the feedback to the durable
        /// rejection record instead of trusting model JSON by itself.
        #[serde(default)]
        feedback: Option<FileRef>,
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
        /// Semantic event version; older records are rejected before replay.
        #[serde(default)]
        schema_version: u32,
        /// Zero-based executor step.
        step: u32,
        /// Stable provider/model-owned call identity.
        call_id: String,
        /// Canonical digest of the complete admitted invocation. Call IDs
        /// alone are insufficient to bind a replayed result to its arguments.
        invocation_digest: [u8; 32],
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

impl ExecutionEvent {
    /// Rejects durable event shapes from before the pinned completion schema.
    pub fn validate_schema_version(&self) -> Result<()> {
        if let Self::ToolCompleted { schema_version, .. } = self
            && *schema_version != TOOL_COMPLETED_EVENT_VERSION
        {
            return Err(Error::Conflict(
                "durable tool completion schema version is unsupported; re-admission is required"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Admission refusals distinct from failures of dispatched effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRejectionKind {
    /// No pinned registered tool exists under this name.
    UnknownTool,
    /// Caller lacks the tool capability.
    Unauthorized,
    /// Resource-specific authorization failed.
    ResourceDenied,
    /// Arguments do not match the pinned schema.
    InvalidArguments,
    /// The pinned policy refused execution.
    PolicyDenied,
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
pub trait ExecutionJournal: Send + Sync {
    /// Implementations must scope sequences and retry keys by `operation_id`.
    /// Replays the complete retained journal before execution resumes.
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
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

/// Complete replaceable turn loop. Implementations may own every policy decision.
pub trait Executor: Send + Sync {
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
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
    limits: Limits,
    tool_scope: RuntimeScope,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
    batch_publisher: Option<Arc<dyn ModelBatchPublisher>>,
    batch_identity: Option<ComponentIdentity>,
    batch_guarantee: Option<EffectGuarantee>,
    authenticated_task: Option<TaskId>,
}

impl StockExecutor {
    fn visible_tool_definitions(&self) -> Result<Vec<crate::tool::ToolDefinition>> {
        Ok(self
            .tools
            .definitions()?
            .into_iter()
            .filter(|tool| {
                self.tool_scope
                    .grants()
                    .contains(&format!("tool:call:{}", tool.name))
            })
            .collect())
    }

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
            provider,
            context,
            tools,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
            batch_publisher: None,
            batch_identity: None,
            batch_guarantee: None,
            authenticated_task: None,
        }
    }

    /// Applies the composition's checked bounds to the stock loop.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Binds model-batch communication to the durable task admitted by the
    /// host. The task identity is transport provenance and cannot be supplied
    /// or changed by a model invocation.
    #[must_use]
    pub fn with_authenticated_task(mut self, task_id: TaskId) -> Self {
        self.authenticated_task = Some(task_id);
        self
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

    /// Binds durable completed-batch publication independently of model content.
    pub fn with_batch_publisher(
        mut self,
        publisher: Option<Arc<dyn ModelBatchPublisher>>,
    ) -> Result<Self> {
        if let Some(provider) = &publisher {
            let identity = provider.identity();
            crate::contract::validate_component_label(&identity.name, "batch publisher name")?;
            crate::contract::validate_component_label(
                &identity.version,
                "batch publisher version",
            )?;
            if identity.digest == [0; 32]
                || (provider.guarantee() == EffectGuarantee::ExactlyOnce
                    && !provider.linearizable_reconciliation())
            {
                return Err(Error::Invalid(
                    "batch publisher must pin a supported guarantee and implementation".into(),
                ));
            }
            self.batch_identity = Some(identity);
            self.batch_guarantee = Some(provider.guarantee());
        }
        self.batch_publisher = publisher;
        Ok(self)
    }

    fn request_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest(&json!({
            "executor": "acyclic.stock.v4",
            "input": input,
            "model": self.model,
            "context": self.context.contracts(),
            "tools": self.tools.definitions()?,
            "limits": self.limits,
            "tool_scope": (self.tool_scope.grants(), self.tool_scope.limits()),
            "policy": self.policy_identity.as_ref(),
            "batch_publisher": (&self.batch_identity, self.batch_guarantee),
            "authenticated_task": self.authenticated_task,
        }))
    }

    fn project_replayed_tool_result(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        result: &ToolResult,
    ) -> Result<Value> {
        validate_value(&tool.definition.output_schema, &result.value, "tool output")?;
        let projection = tool.projection.project(invocation, result)?;
        validate_value(
            &tool.definition.model_output_schema,
            &projection,
            "tool projection",
        )?;
        if crate::contract::canonical_json_bytes(&projection)?.len() as u64
            > self.limits.render_bytes
        {
            return Err(Error::Invalid(
                "tool projection exceeds render limit".into(),
            ));
        }
        Ok(projection)
    }

    /// Replays the durable journal for one turn, verifying it is gapless and bound to the
    /// exact same request, and journals the initial `Started` marker on a fresh turn.
    async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        let records = journal.replay(input.operation_id).await?;
        let mut prepared_steps = BTreeSet::new();
        let mut started_steps = BTreeSet::new();
        let mut started_tools = BTreeSet::new();
        for (index, record) in records.iter().enumerate() {
            record.event.validate_schema_version()?;
            if record.operation_id != input.operation_id || record.sequence != index as u64 + 1 {
                return Err(Error::Conflict(
                    "execution journal is not gapless or belongs to another turn".into(),
                ));
            }
            match &record.event {
                ExecutionEvent::ModelInputPrepared { step, .. } => {
                    if !prepared_steps.insert(*step) {
                        return Err(Error::Storage(
                            "duplicate prepared model input: preparation is duplicated".into(),
                        ));
                    }
                    if started_steps.contains(step) {
                        return Err(Error::Storage(
                            "model input preparation is out of order".into(),
                        ));
                    }
                }
                ExecutionEvent::ModelStarted { step, .. } => {
                    if !prepared_steps.contains(step) {
                        return Err(Error::Storage("model start is missing preparation".into()));
                    }
                    if !started_steps.insert(*step) {
                        return Err(Error::Storage(
                            "model start is duplicated (duplicate model start)".into(),
                        ));
                    }
                }
                ExecutionEvent::Model { step, .. } if !started_steps.contains(step) => {
                    return Err(Error::Storage(
                        "model observation is missing its admitted start".into(),
                    ));
                }
                ExecutionEvent::ToolAdmissionRejected { step, .. }
                    if !started_steps.contains(step) =>
                {
                    return Err(Error::Storage(
                        "tool rejection is missing its admitted model start".into(),
                    ));
                }
                ExecutionEvent::ToolStarted { step, call_id, .. } => {
                    if !started_steps.contains(step) {
                        return Err(Error::Storage(
                            "tool start is missing its admitted model start".into(),
                        ));
                    }
                    if !started_tools.insert((*step, call_id.clone())) {
                        return Err(Error::Storage("tool admission is duplicated".into()));
                    }
                }
                ExecutionEvent::ToolCompleted { step, call_id, .. }
                | ExecutionEvent::ToolFailed { step, call_id, .. }
                    if !started_tools.contains(&(*step, call_id.clone())) =>
                {
                    return Err(Error::Storage(
                        "tool completion is missing its admitted start".into(),
                    ));
                }
                _ => {}
            }
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

    /// Resolves one model step's events, replaying an already completed or started attempt
    /// from the durable journal exactly once instead of re-invoking the provider.
    #[allow(
        clippy::too_many_lines,
        reason = "one exactly-once replay-or-generate operation for a single model step \
                  (already-completed replay, in-flight reconcile, or fresh generate, each \
                  interleaved with journal appends); splitting the branches further would \
                  fragment one atomic step across more functions without clarifying it"
    )]
    async fn run_model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        rejection_evidence: &[crate::tool::ToolRejectionFeedback],
        prior_output_bytes: u64,
    ) -> Result<Vec<ModelEvent>> {
        let records = journal.replay(input.operation_id).await?;
        let persisted_prepared = prepared_model_input(&records, step)?;
        let mut started = None;
        for record in &records {
            match &record.event {
                ExecutionEvent::ModelStarted {
                    step: event_step,
                    request_digest,
                } if *event_step == step => {
                    if started.replace(*request_digest).is_some() {
                        return Err(Error::Storage(
                            "duplicate model start for executor step".into(),
                        ));
                    }
                }
                _ => {}
            }
        }

        let prepared = if let Some((manifest_ref, request_ref)) = persisted_prepared {
            // A prepared request is the durable boundary for model input. Never
            // re-run context stages on replay: they may read mutable files or
            // perform retrieval, which would silently change the provider bytes.
            let manifest =
                load_json::<crate::model_input::ModelInputManifest>(journal, &manifest_ref).await?;
            let request = load_json::<ModelRequest>(journal, &request_ref).await?;
            let prepared = crate::model_input::PreparedModelInput::prepare_with_policy(
                request.clone(),
                self.limits,
                self.provider.model_option_policy(),
            )?;
            let current_tools = self
                .tools
                .definitions()?
                .into_iter()
                .filter(|tool| {
                    self.tool_scope
                        .grants()
                        .contains(&format!("tool:call:{}", tool.name))
                })
                .collect::<Vec<_>>();
            if request.model != self.model || request.tools != current_tools {
                return Err(Error::Conflict(
                    "persisted model input component bindings changed".into(),
                ));
            }
            if manifest.version != crate::model_input::MODEL_INPUT_VERSION {
                return Err(Error::Conflict(
                    "persisted model input manifest no longer matches its request".into(),
                ));
            }
            let prepared = prepared.with_rejection_evidence(manifest.rejection_evidence.clone())?;
            if manifest != *prepared.manifest() {
                return Err(Error::Conflict(
                    "persisted model input manifest no longer matches its request".into(),
                ));
            }
            prepared.validate_complete_exchange()?;
            self.provider.admit(&request)?;
            for message in &request.messages {
                message.content.validate_limits(self.limits)?;
                for reference in crate::model_input::message_file_refs(message, &request.tools)? {
                    journal.verify_input_file(&reference).await?;
                }
            }
            prepared
        } else {
            let context = self
                .context
                .run_with_rejection_evidence(
                    &ContextInput {
                        input: input.input.clone(),
                        selected_context: input.selected_context.clone(),
                        step,
                        prior_messages: prior_messages.to_vec(),
                    },
                    rejection_evidence,
                )
                .await?;
            if context.messages.len() > self.limits.context_messages {
                return Err(Error::Invalid("model context exceeds message limit".into()));
            }
            let tools = self.visible_tool_definitions()?;
            for message in &context.messages {
                message.content.validate_limits(self.limits)?;
                // Stages may introduce references beyond the original turn selection.
                // Resolve each final reference under this journal's exact authority
                // before admission, reconciliation, or dispatch reaches a provider.
                for reference in crate::model_input::message_file_refs(message, &tools)? {
                    journal.verify_input_file(&reference).await?;
                }
            }
            let prepared = crate::model_input::PreparedModelInput::prepare_with_policy(
                ModelRequest {
                    model: self.model.clone(),
                    messages: context.messages,
                    tools,
                    max_output_tokens: None,
                },
                self.limits,
                self.provider.model_option_policy(),
            )?
            .with_rejection_evidence(context.rejection_evidence.clone())?;
            prepared.validate_complete_exchange()?;
            self.provider.admit(prepared.request())?;
            let manifest_key = format!("model:{step}:input");
            let manifest = stage_json(
                journal,
                input.operation_id,
                &manifest_key,
                prepared.manifest(),
            )
            .await?;
            let request_file = stage_json(
                journal,
                input.operation_id,
                &format!("model:{step}:request"),
                prepared.request(),
            )
            .await?;
            journal
                .append(
                    input.operation_id,
                    manifest_key,
                    ExecutionEvent::ModelInputPrepared {
                        step,
                        manifest,
                        request: request_file,
                    },
                )
                .await?;
            prepared
        };
        let mut replayed_model = Vec::new();
        let mut admission = ModelEventAdmission::default();
        let mut output_bytes = prior_output_bytes;
        for record in &records {
            if let ExecutionEvent::Model {
                step: event_step,
                event,
            } = &record.event
                && *event_step == step
            {
                let event = load_json::<ModelEvent>(journal, event).await?;
                admission.observe(&event, self.limits)?;
                admit_model_output_bytes(&mut output_bytes, &event, self.limits)?;
                replayed_model.push(event);
            }
        }
        let request_digest = prepared.manifest().request_digest;
        if started.is_none() && !replayed_model.is_empty() {
            return Err(Error::Storage(
                "model observations exist without an admitted attempt".into(),
            ));
        }
        if started.is_some_and(|existing_digest| existing_digest != request_digest) {
            return Err(Error::Conflict(
                "model attempt identity is bound to another request".into(),
            ));
        }
        let replay_completed = admission.completed;
        let model_events = if replay_completed {
            replayed_model
        } else if started.is_some() {
            let Some(mut continuation) = self
                .provider
                .reconcile_admitted(
                    prepared.clone(),
                    ModelAttempt {
                        operation_id: input.operation_id,
                        step,
                        request_digest,
                        observed: replayed_model.clone(),
                    },
                )
                .await?
            else {
                return Err(Error::Indeterminate(input.operation_id));
            };
            let mut observed = replayed_model;
            for event in continuation.drain(..) {
                admission.observe(&event, self.limits)?;
                admit_model_output_bytes(&mut output_bytes, &event, self.limits)?;
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
            let current = journal.replay(input.operation_id).await?;
            let current_starts = current
                .iter()
                .filter_map(|record| match &record.event {
                    ExecutionEvent::ModelStarted {
                        step: event_step,
                        request_digest,
                    } if *event_step == step => Some(*request_digest),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if current_starts.len() > 1 {
                return Err(Error::Storage(
                    "duplicate model start for executor step".into(),
                ));
            }
            if let Some(existing) = current_starts.first() {
                if *existing != request_digest {
                    return Err(Error::Conflict(
                        "model attempt identity is bound to another request".into(),
                    ));
                }
                return Err(Error::Indeterminate(input.operation_id));
            }
            let claimed = journal
                .append_if_tail(
                    input.operation_id,
                    current.len() as u64,
                    format!("model:{step}:claim:{}", OperationId::new()),
                    ExecutionEvent::ModelStarted {
                        step,
                        request_digest,
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
            crate::stack_diagnostics::marker("provider-dispatch-enter");
            let mut stream = self.provider.generate(prepared);
            let mut observed = Vec::new();
            while let Some(event) = stream.next().await {
                let event = event?;
                admission.observe(&event, self.limits)?;
                admit_model_output_bytes(&mut output_bytes, &event, self.limits)?;
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
            crate::stack_diagnostics::marker("provider-dispatch-complete");
            observed
        };
        Ok(model_events)
    }

    async fn record_tool_rejection(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        invocation: &ToolInvocation,
        reason: ToolRejectionKind,
        feedback: Option<&crate::tool::ToolRejectionFeedback>,
    ) -> Result<()> {
        let key = format!("tool:{step}:{}:rejected", invocation.call_id);
        let reference = stage_json(journal, operation, &key, invocation).await?;
        let feedback = match feedback {
            Some(feedback) => Some(
                stage_json(
                    journal,
                    operation,
                    &format!("tool:{step}:{}:rejection-feedback", invocation.call_id),
                    feedback,
                )
                .await?,
            ),
            None => None,
        };
        journal
            .append(
                operation,
                key,
                ExecutionEvent::ToolAdmissionRejected {
                    step,
                    invocation: reference,
                    reason,
                    feedback,
                },
            )
            .await
    }

    async fn record_completed_batch(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        completed: &[ModelMessage],
    ) -> Result<()> {
        let records = journal.replay(operation).await?;
        let (manifest_file, request_file) = prepared_model_input(&records, step)?
            .ok_or_else(|| Error::Storage("completed batch has no pinned request".into()))?;
        let mut request: ModelRequest = load_json(journal, &request_file).await?;
        let manifest: crate::model_input::ModelInputManifest =
            load_json(journal, &manifest_file).await?;
        let prepared = crate::model_input::PreparedModelInput::prepare_with_policy(
            request.clone(),
            self.limits,
            self.provider.model_option_policy(),
        )?
        .with_rejection_evidence(manifest.rejection_evidence.clone())?;
        if manifest != *prepared.manifest() {
            return Err(Error::Conflict(
                "completed batch model input bindings changed".into(),
            ));
        }
        request.messages.extend_from_slice(completed);
        let mut rejections = manifest.rejection_evidence;
        for record in &records {
            let ExecutionEvent::ToolAdmissionRejected {
                step: rejected_step,
                reason: ToolRejectionKind::InvalidArguments,
                feedback: Some(feedback),
                ..
            } = &record.event
            else {
                continue;
            };
            if *rejected_step != step {
                continue;
            }
            let feedback = load_json(journal, feedback).await?;
            // The manifest covers the request prefix; this step's records
            // cover only the appended exchange. Equal envelopes at different
            // steps are distinct occurrences and must retain multiplicity.
            rejections.push(feedback);
        }
        let boundary = crate::model_input::CompletedModelBoundary::capture_with_policy(
            request,
            self.limits,
            self.provider.model_option_policy(),
            &rejections,
        )?;
        let key = format!("model:{step}:completed-batch");
        let reference = stage_json(journal, operation, &key, &boundary).await?;
        journal
            .append(
                operation,
                key,
                ExecutionEvent::ToolBatchCompleted {
                    step,
                    boundary: reference.clone(),
                },
            )
            .await?;
        self.publish_completed_batch(journal, operation, step, request_file.clone(), reference)
            .await
    }

    fn validate_batch_publisher(&self, publisher: &dyn ModelBatchPublisher) -> Result<()> {
        if self.batch_identity.as_ref() != Some(&publisher.identity())
            || self.batch_guarantee != Some(publisher.guarantee())
        {
            return Err(Error::Conflict(
                "batch publisher changed after binding".into(),
            ));
        }
        Ok(())
    }

    async fn publish_completed_batch(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        request: FileRef,
        boundary: FileRef,
    ) -> Result<()> {
        let Some(publisher) = &self.batch_publisher else {
            return Ok(());
        };
        self.validate_batch_publisher(publisher.as_ref())?;
        let identity = publisher.identity();
        let guarantee = publisher.guarantee();
        let publication = ModelBatchPublication {
            operation_id: ModelToolContext {
                parent_operation: operation,
                step,
                task_id: None,
            }
            .publication_operation(),
            parent_operation: operation,
            step,
            request,
            boundary,
            publisher: identity,
            guarantee,
        };
        let digest = crate::contract::canonical_json_digest(&publication)?;
        let records = journal.replay(operation).await?;
        let started = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::BatchPublicationStarted {
                step: recorded,
                publication,
            } if *recorded == step => Some(publication),
            _ => None,
        });
        if let Some(file) = started
            && load_json::<ModelBatchPublication>(journal, file).await? != publication
        {
            return Err(Error::Conflict(
                "batch publication admission changed".into(),
            ));
        }
        if let Some(completed) = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::BatchPublicationCompleted {
                step: recorded,
                publication_digest,
            } if *recorded == step => Some(publication_digest),
            _ => None,
        }) {
            return if started.is_some() && completed == &digest {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "batch publication result has no matching admission".into(),
                ))
            };
        }
        if started.is_some() {
            if publisher.reconcile(publication.clone()).await?.is_none() {
                if guarantee != EffectGuarantee::IdempotentRetry {
                    return Err(Error::Indeterminate(publication.operation_id));
                }
                crate::stack_diagnostics::marker("fork-publication-enter-retry");
                let publish = publisher.publish(publication.clone());
                crate::stack_diagnostics::future_size("fork-publication-handle-retry", &publish);
                crate::stack_diagnostics::future_size(
                    "fork-publication-inner-retry",
                    &*publish,
                );
                publish.await?;
                crate::stack_diagnostics::marker("fork-publication-complete-retry");
            }
        } else {
            let key = format!("model:{step}:publication");
            let file = stage_json(journal, operation, &key, &publication).await?;
            if !journal
                .append_if_tail(
                    operation,
                    records.len() as u64,
                    key,
                    ExecutionEvent::BatchPublicationStarted {
                        step,
                        publication: file,
                    },
                )
                .await?
            {
                return Err(Error::Indeterminate(publication.operation_id));
            }
            crate::stack_diagnostics::marker("fork-publication-enter");
            let publish = publisher.publish(publication.clone());
            crate::stack_diagnostics::future_size("fork-publication-handle", &publish);
            crate::stack_diagnostics::future_size("fork-publication-inner", &*publish);
            publish.await?;
            crate::stack_diagnostics::marker("fork-publication-complete");
        }
        if publisher.identity() != publication.publisher
            || publisher.guarantee() != publication.guarantee
        {
            return Err(Error::Conflict(
                "batch publisher changed during publication".into(),
            ));
        }
        journal
            .append(
                operation,
                format!("model:{step}:publication-complete"),
                ExecutionEvent::BatchPublicationCompleted {
                    step,
                    publication_digest: digest,
                },
            )
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
        let current = journal.replay(operation_id).await?;
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
                current.len() as u64,
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

    /// Resolves one tool invocation against the durable journal, replaying an already
    /// completed or started attempt exactly once, and appends the resulting message.
    #[allow(
        clippy::too_many_lines,
        reason = "one exactly-once replay-or-execute operation for a single tool call \
                  (already-completed replay, in-flight reconcile, or fresh execute, each \
                  interleaved with journal appends); splitting the branches further would \
                  fragment one atomic invocation across more functions without clarifying it"
    )]
    async fn resolve_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<Option<ToolRejectionFeedback>> {
        let records = journal.replay(operation_id).await?;
        invocation.validate()?;
        let mut started = None;
        let mut completed_tool = None;
        let mut retained_rejection = None;
        let mut failed_tool = None;
        for record in &records {
            match &record.event {
                ExecutionEvent::ToolStarted {
                    step: event_step,
                    call_id,
                    invocation: existing,
                } if *event_step == step && call_id == &invocation.call_id => {
                    if started.is_some() {
                        return Err(Error::Storage("duplicate admitted tool invocation".into()));
                    }
                    let existing = load_json::<ToolInvocation>(journal, existing).await?;
                    if existing != invocation {
                        return Err(Error::Conflict(
                            "tool call identity is bound to another invocation".into(),
                        ));
                    }
                    started = Some(());
                }
                ExecutionEvent::ToolCompleted {
                    step: event_step,
                    call_id,
                    result,
                    projection,
                    invocation_digest,
                    ..
                } if *event_step == step && call_id == &invocation.call_id => {
                    if completed_tool.is_some() {
                        return Err(Error::Storage("duplicate completed tool result".into()));
                    }
                    completed_tool = Some((result.clone(), projection.clone(), *invocation_digest));
                }
                ExecutionEvent::ToolAdmissionRejected {
                    step: event_step,
                    invocation: existing,
                    reason,
                    feedback,
                } if *event_step == step => {
                    let existing = load_json::<ToolInvocation>(journal, existing).await?;
                    if existing.call_id != invocation.call_id {
                        continue;
                    }
                    if existing != invocation {
                        return Err(Error::Conflict(
                            "rejected tool call identity is bound to another invocation".into(),
                        ));
                    }
                    if retained_rejection.is_some() {
                        return Err(Error::Storage("duplicate tool admission rejection".into()));
                    }
                    retained_rejection = Some((*reason, feedback.clone()));
                }
                ExecutionEvent::ToolFailed {
                    step: event_step,
                    call_id,
                    reason,
                } if *event_step == step && call_id == &invocation.call_id => {
                    if failed_tool.is_some() {
                        return Err(Error::Storage("duplicate failed tool record".into()));
                    }
                    failed_tool = Some(*reason);
                }
                _ => {}
            }
        }
        if completed_tool.is_some() && (retained_rejection.is_some() || failed_tool.is_some()) {
            return Err(Error::Storage(
                "tool journal contains contradictory terminal records".into(),
            ));
        }
        if retained_rejection.is_some() && failed_tool.is_some() {
            return Err(Error::Storage(
                "tool journal contains contradictory terminal records".into(),
            ));
        }
        if retained_rejection.is_some() && started.is_some() {
            return Err(Error::Storage(
                "tool journal contains contradictory admission records".into(),
            ));
        }
        if completed_tool.is_some() && started.is_none() {
            return Err(Error::Invalid(
                "completed tool has no admitted invocation".into(),
            ));
        }
        let tool = self.tools.get(&invocation.name);
        if let Some((reason, feedback)) = retained_rejection {
            match reason {
                ToolRejectionKind::InvalidArguments => {
                    let tool = tool.ok_or_else(|| {
                        Error::Storage("invalid-argument rejection lacks pinned tool".into())
                    })?;
                    let error_text = match validate_value(
                        &tool.definition.input_schema,
                        &invocation.arguments,
                        "tool input",
                    ) {
                        Ok(()) => {
                            return Err(Error::Conflict(
                                "durable invalid-argument rejection no longer applies".into(),
                            ));
                        }
                        Err(error) => error.to_string(),
                    };
                    let feedback = feedback.ok_or_else(|| {
                        Error::Storage("invalid-argument rejection lacks feedback".into())
                    })?;
                    let durable: ToolRejectionFeedback = load_json(journal, &feedback).await?;
                    let expected = ToolRejectionFeedback::invalid_arguments(
                        &invocation,
                        &tool.definition.input_schema,
                        &error_text,
                    )?;
                    if durable != expected {
                        return Err(Error::Conflict("durable rejection feedback changed".into()));
                    }
                    let message = ModelMessage {
                        role: ModelRole::Tool,
                        content: ModelContent::Part(ModelContentPart::ToolResult {
                            call_id: invocation.call_id.clone(),
                            name: invocation.name.clone(),
                            value: durable.to_model_value(&error_text)?,
                        }),
                    };
                    message.content.validate_limits(self.limits)?;
                    prior_messages.push(message);
                    return Ok(Some(durable));
                }
                ToolRejectionKind::UnknownTool => {
                    return Err(Error::NotFound(format!("tool {}", invocation.name)));
                }
                ToolRejectionKind::Unauthorized | ToolRejectionKind::ResourceDenied => {
                    return Err(Error::Unauthorized(
                        "durable tool admission was denied".into(),
                    ));
                }
                ToolRejectionKind::PolicyDenied => {
                    return Err(Error::Unauthorized(
                        "durable tool policy denied admission".into(),
                    ));
                }
            }
        }
        let Some(tool) = tool else {
            self.record_tool_rejection(
                journal,
                operation_id,
                step,
                &invocation,
                ToolRejectionKind::UnknownTool,
                None,
            )
            .await?;
            return Err(Error::NotFound(format!("tool {}", invocation.name)));
        };
        if let Some((result_ref, projection_ref, invocation_digest)) = completed_tool {
            if invocation_digest != crate::contract::canonical_json_digest(&invocation)? {
                return Err(Error::Conflict(
                    "completed tool result is bound to another invocation".into(),
                ));
            }
            let result: ToolResult = load_json(journal, &result_ref).await?;
            let projection = self.project_replayed_tool_result(tool, &invocation, &result)?;
            let persisted_projection: Value = load_json(journal, &projection_ref).await?;
            if crate::contract::canonical_json_bytes(&persisted_projection)?
                != crate::contract::canonical_json_bytes(&projection)?
            {
                return Err(Error::Conflict(
                    "completed tool projection differs from pinned projection".into(),
                ));
            }
            let message = ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: invocation.call_id.clone(),
                    name: invocation.name.clone(),
                    value: projection,
                }),
            };
            message.content.validate_limits(self.limits)?;
            prior_messages.push(message);
            return Ok(None);
        }
        // Authorization precedes argument validation, and must stay that way. A validation error
        // describes the tool's pinned input schema, so answering one for a tool the caller was
        // never granted would let a model probe the contract of an ungranted tool by naming it
        // with deliberately malformed arguments. An ungranted call is refused on its own terms,
        // whatever its arguments look like.
        let capability = format!("tool:call:{}", tool.definition.name);
        if !self.tool_scope.grants().contains(&capability) {
            self.record_tool_rejection(
                journal,
                operation_id,
                step,
                &invocation,
                ToolRejectionKind::Unauthorized,
                None,
            )
            .await?;
            return Err(Error::Unauthorized(format!("scope lacks {capability}")));
        }
        if let Err(error) = tool.executor.authorize(Some(&self.tool_scope), &invocation) {
            self.record_tool_rejection(
                journal,
                operation_id,
                step,
                &invocation,
                ToolRejectionKind::ResourceDenied,
                None,
            )
            .await?;
            return Err(error);
        }
        // Malformed arguments are the model's mistake to correct, not a reason to end the turn:
        // hand the validation message back as this call's own result so the next step can fix
        // them. Ending the turn instead makes the most recoverable failure in the loop fatal, and
        // the replacement agent â€” fresh context, same model, same schema â€” repeats it exactly.
        //
        // Admission refusals are durable, separately from effect dispatch.
        // The model-visible validation message is derived from its pinned schema.
        if let Err(error) = validate_value(
            &tool.definition.input_schema,
            &invocation.arguments,
            "tool input",
        ) {
            let error_text = error.to_string();
            let feedback = ToolRejectionFeedback::invalid_arguments(
                &invocation,
                &tool.definition.input_schema,
                &error_text,
            )?;
            self.record_tool_rejection(
                journal,
                operation_id,
                step,
                &invocation,
                ToolRejectionKind::InvalidArguments,
                Some(&feedback),
            )
            .await?;
            let message = ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: invocation.call_id.clone(),
                    name: invocation.name.clone(),
                    value: feedback.to_model_value(&error_text)?,
                }),
            };
            message.content.validate_limits(self.limits)?;
            prior_messages.push(message);
            return Ok(Some(feedback));
        }
        let started = started.is_some();
        if let Some(reason) = failed_tool {
            return Err(Error::Invalid(reason.message().into()));
        }
        let (result, projection) = {
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
                            self.record_tool_rejection(
                                journal,
                                operation_id,
                                step,
                                &invocation,
                                ToolRejectionKind::PolicyDenied,
                                None,
                            )
                            .await?;
                            return Err(Error::Unauthorized(reason));
                        }
                        ToolPolicyDecision::RequireApproval { prompt } => {
                            if !scope.grants().contains("interaction:route") {
                                return Err(Error::Unauthorized(
                                    "stock tool approval requires interaction:route".into(),
                                ));
                            }
                            let digest = crate::contract::canonical_json_digest(&(
                                &operation_id,
                                step,
                                &tool.definition,
                                &invocation,
                            ))?;
                            let mut identity = [0_u8; 16];
                            identity.copy_from_slice(
                                &blake3::hash(
                                    &[
                                        b"harness:stock-tool-approval:v2".as_slice(),
                                        operation_id.into_bytes().as_slice(),
                                        &step.to_be_bytes(),
                                        invocation.call_id.as_bytes(),
                                    ]
                                    .concat(),
                                )
                                .as_bytes()[..16],
                            );
                            let approval = InteractionId::from_bytes(identity);
                            journal
                                .open_interaction(
                                    approval,
                                    Interaction::approval(prompt, operation_id, digest)?,
                                )
                                .await?;
                            check_tool_approval(
                                journal
                                    .interaction_outcome(approval)
                                    .await?
                                    .unwrap_or(InteractionOutcome::Indeterminate { operation_id }),
                            )?;
                        }
                    }
                }
            }
            let claimed = if started {
                false
            } else {
                let invocation_ref = stage_json(
                    journal,
                    operation_id,
                    &format!("tool:{step}:{}:invocation", invocation.call_id),
                    &invocation,
                )
                .await?;
                let current = journal.replay(operation_id).await?;
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
                        current.len() as u64,
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
            let tool_context = ModelToolContext {
                parent_operation: operation_id,
                step,
                task_id: self.authenticated_task,
            };
            tool_context.validate_invocation(&invocation)?;
            let result = if claimed {
                match tool
                    .executor
                    .execute_in_model_batch(tool_context, invocation.clone())
                    .await
                {
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
                match tool
                    .executor
                    .reconcile_in_model_batch(tool_context, invocation.clone())
                    .await
                {
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
                    validate_value(
                        &tool.definition.model_output_schema,
                        &value,
                        "tool projection",
                    )?;
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
            let current = journal.replay(operation_id).await?;
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
                    current.len() as u64,
                    format!(
                        "tool:{step}:{}:completed:{}",
                        invocation.call_id,
                        OperationId::new()
                    ),
                    ExecutionEvent::ToolCompleted {
                        schema_version: TOOL_COMPLETED_EVENT_VERSION,
                        step,
                        call_id: invocation.call_id.clone(),
                        invocation_digest: crate::contract::canonical_json_digest(&invocation)?,
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
        let message = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: invocation.call_id.clone(),
                name: invocation.name.clone(),
                value: projection,
            }),
        };
        message.content.validate_limits(self.limits)?;
        prior_messages.push(message);
        Ok(None)
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
        input.input.validate_limits(self.limits)?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input)?;
            if selected.messages.len() > self.limits.context_messages {
                return Err(Error::Invalid(
                    "selected context exceeds configured message limit".into(),
                ));
            }
            journal
                .verify_selected_context(input.operation_id, selected)
                .await?;
            let tools = self.visible_tool_definitions()?;
            for message in &selected.messages {
                message.content.validate_limits(self.limits)?;
                for reference in crate::model_input::message_file_refs(message, &tools)? {
                    journal.verify_input_file(&reference).await?;
                }
            }
        }
        for reference in input.input.file_refs() {
            journal.verify_input_file(reference).await?;
        }
        Ok(())
    }
}

impl Executor for StockExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move {
            self.validate_turn_input(journal, &input).await?;
            self.ensure_started(journal, &input).await?;
            let mut prior_messages = Vec::new();
            let mut rejection_evidence = input
                .selected_context
                .as_ref()
                .map(|context| context.rejection_evidence.clone())
                .unwrap_or_default();
            let mut text = String::new();
            // Text emitted alongside a tool batch is part of the authoritative
            // conversation, but it is not the final user-visible completion.
            // Keep it in `text` for replay and output accounting while tracking
            // the terminal step separately for the returned result.
            let mut visible_text = String::new();
            for step in 0..input.max_steps {
                let step_text_start = text.len();
                let mut calls = Vec::new();
                let mut completed = None;
                let model_step = self.run_model_step(
                    journal,
                    &input,
                    step,
                    &prior_messages,
                    &rejection_evidence,
                    text.len() as u64,
                );
                crate::stack_diagnostics::future_size("run-model-step", &model_step);
                let model_events = model_step.await?;
                for event in model_events {
                    match event {
                        ModelEvent::Content { delta } => {
                            visible_text.push_str(&delta);
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
                    return Ok(TurnOutput {
                        text: visible_text,
                        attachments: Vec::new(),
                        metadata,
                        steps: step + 1,
                    });
                }
                let batch_start = prior_messages.len();
                if text.len() > step_text_start {
                    prior_messages.push(ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text(
                            text.get(step_text_start..)
                                .ok_or_else(|| {
                                    Error::Storage("assistant text boundary is invalid".into())
                                })?
                                .to_owned(),
                        ),
                    });
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
                    if let Some(feedback) = self
                        .resolve_tool_call(
                            journal,
                            input.operation_id,
                            step,
                            invocation,
                            &mut prior_messages,
                        )
                        .await?
                    {
                        rejection_evidence.push(feedback);
                    }
                }
                let completed = prior_messages
                    .get(batch_start..)
                    .ok_or_else(|| Error::Storage("completed batch range is invalid".into()))?;
                self.record_completed_batch(journal, input.operation_id, step, completed)
                    .await?;
                visible_text.clear();
            }
            Err(Error::Conflict("executor step limit reached".into()))
        })
    }
}

fn prepared_model_input(
    records: &[ExecutionRecord],
    step: u32,
) -> Result<Option<(FileRef, FileRef)>> {
    let mut prepared = None;
    for record in records {
        if let ExecutionEvent::ModelInputPrepared {
            step: recorded,
            manifest,
            request,
        } = &record.event
        {
            if *recorded == step {
                if prepared.is_some() {
                    return Err(Error::Storage(
                        "duplicate prepared model input for executor step".into(),
                    ));
                }
                prepared = Some((manifest.clone(), request.clone()));
            }
        }
    }
    Ok(prepared)
}

pub(crate) async fn stage_json<T: Serialize>(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    key: &str,
    value: &T,
) -> Result<FileRef> {
    let bytes = crate::contract::canonical_json_bytes(value)?;
    let reference = journal
        .stage(operation_id, key.into(), bytes.clone(), "application/json")
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(Error::Storage(
            "execution journal returned a non-private JSON reference".into(),
        ));
    }
    reference.descriptor().verify(&bytes)?;
    Ok(reference)
}

pub(crate) async fn load_json<T: serde::de::DeserializeOwned>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
) -> Result<T> {
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(Error::Storage(
            "execution journal references non-private JSON content".into(),
        ));
    }
    let bytes = journal.load(reference).await?;
    reference.descriptor().verify(&bytes).map_err(|error| {
        Error::Storage(format!(
            "execution journal content descriptor is invalid: {error}"
        ))
    })?;
    let parsed: Value = serde_json::from_slice(&bytes)
        .map_err(|error| Error::Storage(format!("execution journal JSON is invalid: {error}")))?;
    if crate::contract::canonical_json_bytes(&parsed)? != bytes {
        return Err(Error::Storage(
            "execution journal JSON is not canonical".into(),
        ));
    }
    serde_json::from_slice(&bytes)
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

    pub(crate) fn observe(&mut self, event: &ModelEvent, limits: Limits) -> Result<()> {
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
                if self.calls.len() >= limits.tool_calls_per_step
                    || !self.calls.insert(call_id.clone())
                {
                    return Err(Error::Invalid(
                        "model tool call limit exceeded or identity repeated".into(),
                    ));
                }
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

/// Checks cumulative user-visible model output before an event is journaled.
/// Reasoning is intentionally excluded because it never enters `TurnOutput`;
/// content bytes are counted across model steps by the executor.
fn admit_model_output_bytes(
    output_bytes: &mut u64,
    event: &ModelEvent,
    limits: Limits,
) -> Result<()> {
    let ModelEvent::Content { delta } = event else {
        return Ok(());
    };
    *output_bytes = output_bytes
        .checked_add(delta.len() as u64)
        .ok_or_else(|| Error::Invalid("model output size overflow".into()))?;
    if *output_bytes > limits.file_bytes {
        return Err(Error::Invalid("assistant output exceeds file limit".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        context::{Context, ContextStage},
        conversation::{FileDescriptor, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use futures::{FutureExt as _, stream};
    use std::collections::HashMap;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    /// Emits two malformed calls â€” including `parameters` where the pinned
    /// schema expects a direct argument â€” then a well-formed call after both
    /// durable rejection envelopes have been returned.
    struct SlippingModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for SlippingModel {
        fn generate<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            let request = prepared.request().clone();
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
                    Ok(ModelEvent::ToolCall {
                        call_id: "call-2".into(),
                        name: "example.echo".into(),
                        arguments: json!({"unexpected": true}),
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

    impl ModelProvider for FakeModel {
        fn generate<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            let request = prepared.request().clone();
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

    struct ProjectionModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for ProjectionModel {
        fn generate<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            let request = prepared.request().clone();
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut requests) = self.requests.lock() {
                requests.push(request);
            }
            let events = if call == 0 {
                vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "private-call".into(),
                        name: "example.private".into(),
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
    }

    struct ReplayModel {
        calls: AtomicUsize,
    }

    struct OversizedOutputModel {
        calls: AtomicUsize,
    }

    struct PersistedCaptureModel {
        generate_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        seen: Mutex<Vec<(Vec<u8>, crate::model_input::ModelInputManifest)>>,
    }

    impl ModelProvider for ReplayModel {
        fn generate<'a>(
            &'a self,
            _: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "done".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({"finish": "stop"}),
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    impl ModelProvider for OversizedOutputModel {
        fn generate<'a>(
            &'a self,
            _: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "x".repeat(4_097),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    impl ModelProvider for PersistedCaptureModel {
        fn generate<'a>(
            &'a self,
            _: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            self.generate_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "partial-".into(),
                }),
                Err(Error::Storage("stream interrupted".into())),
            ]))
        }

        fn reconcile_admitted<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
            attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            let bytes = prepared.bytes().to_vec();
            let manifest = prepared.manifest().clone();
            async move {
                if manifest.request_digest != attempt.request_digest {
                    return Err(Error::Conflict(
                        "reconciliation request digest changed".into(),
                    ));
                }
                self.seen
                    .lock()
                    .map_err(|_| Error::Storage("capture lock poisoned".into()))?
                    .push((bytes, manifest));
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

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct CountingContext {
        runs: Arc<AtomicUsize>,
    }

    impl ContextStage for CountingContext {
        fn name(&self) -> &str {
            "test.counting-context"
        }

        fn contract(&self) -> Value {
            json!({"name": self.name(), "revision": 1})
        }

        fn apply<'a>(
            &'a self,
            _: &'a ContextInput,
            context: Context,
        ) -> BoxFuture<'a, Result<Context>> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            async move { Ok(context) }.boxed()
        }
    }

    impl ModelProvider for RecoverableModel {
        fn generate<'a>(
            &'a self,
            _: crate::model_input::PreparedModelInput,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            self.generate_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "partial-".into(),
                }),
                Err(Error::Storage("stream interrupted".into())),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt.observed
                    != vec![ModelEvent::Content {
                        delta: "partial-".into(),
                    }]
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

    struct PrivateResultTool;

    impl crate::tool::ToolExecutor for PrivateResultTool {
        fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            async {
                Ok(ToolResult {
                    value: json!({"private": "secret", "public": "shown"}),
                })
            }
            .boxed()
        }

        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct NarrowProjection;

    impl crate::tool::ToolProjection for NarrowProjection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(json!({"public": result.value["public"]}))
        }
    }

    struct InterruptedContextTool {
        executions: Mutex<Vec<(ModelToolContext, ToolInvocation)>>,
        reconciliations: Mutex<Vec<(ModelToolContext, ToolInvocation)>>,
    }

    impl crate::tool::ToolExecutor for InterruptedContextTool {
        fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            async { Err(Error::Invalid("missing model tool provenance".into())) }.boxed()
        }

        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Err(Error::Invalid("missing recovery provenance".into())) }.boxed()
        }

        fn execute_in_model_batch<'a>(
            &'a self,
            context: ModelToolContext,
            invocation: ToolInvocation,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            async move {
                context.validate_invocation(&invocation)?;
                self.executions.lock().unwrap().push((context, invocation));
                Err(Error::Storage("lost tool admission response".into()))
            }
            .boxed()
        }

        fn reconcile_in_model_batch<'a>(
            &'a self,
            context: ModelToolContext,
            invocation: ToolInvocation,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async move {
                context.validate_invocation(&invocation)?;
                self.reconciliations
                    .lock()
                    .unwrap()
                    .push((context, invocation.clone()));
                Ok(Some(ToolResult {
                    value: invocation.arguments,
                }))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn old_publication_identity_semantics_are_fenced_before_dispatch() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::new(),
        );
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("same".into()),
            selected_context: None,
            max_steps: 1,
        };
        let old_digest = crate::contract::canonical_json_digest(&json!({
            "executor": "acyclic.stock.v3",
            "input": input,
            "model": executor.model,
            "context": executor.context.contracts(),
            "tools": executor.tools.definitions()?,
            "limits": executor.limits,
            "tool_scope": (executor.tool_scope.grants(), executor.tool_scope.limits()),
            "policy": executor.policy_identity.as_ref(),
            "batch_publisher": (&executor.batch_identity, executor.batch_guarantee),
        }))?;
        assert_ne!(old_digest, executor.request_digest(&input)?);
        let journal = Journal::default();
        journal
            .append(
                input.operation_id,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: old_digest,
                },
            )
            .await?;
        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        assert_eq!(journal.0.lock().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn model_tool_provenance_survives_recovery_without_entering_model_input() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool = Arc::new(InterruptedContextTool {
            executions: Mutex::new(Vec::new()),
            reconciliations: Mutex::new(Vec::new()),
        });
        let publisher = Arc::new(InterruptedPublisher {
            guarantee: EffectGuarantee::AtMostOnce,
            observed: true,
            dispatches: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
            admissions: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                model_output_schema: json!({"type":"object"}),
            },
            executor: tool.clone(),
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
        )?
        .with_authenticated_task(TaskId::from_bytes([33; 16]))
        .with_batch_publisher(Some(publisher.clone()))?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("exact input".into()),
            selected_context: None,
            max_steps: 2,
        };
        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Indeterminate(_))
        ));
        let first = tool.executions.lock().unwrap()[0].clone();
        assert_eq!(
            first.0,
            ModelToolContext {
                parent_operation: input.operation_id,
                step: 0,
                task_id: Some(TaskId::from_bytes([33; 16])),
            }
        );
        // Publication itself loses its response on the recovery run.
        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(
            tool.reconciliations.lock().unwrap().as_slice(),
            &[first.clone()]
        );
        assert_eq!(
            publisher.admissions.lock().unwrap()[0].operation_id,
            first.0.publication_operation()
        );
        assert_eq!(
            executor.execute(input.clone(), &journal).await?.text,
            "done"
        );
        executor.execute(input, &journal).await?;
        assert_eq!(tool.executions.lock().unwrap().len(), 1);
        assert_eq!(tool.reconciliations.lock().unwrap().len(), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        let requests = model.requests.lock().unwrap();
        let visible = crate::contract::canonical_json_bytes(&requests[1])?;
        let visible = String::from_utf8(visible).unwrap();
        assert!(!visible.contains("parent_operation"));
        assert!(!visible.contains(&first.0.parent_operation.to_string()));
        assert!(!visible.contains(&first.0.publication_operation().to_string()));
        assert!(requests[1].messages.iter().any(|message| matches!(
            &message.content,
            ModelContent::Part(ModelContentPart::ToolCall { call_id, arguments, .. })
                if call_id == "call-1" && *arguments == first.1.arguments
        )));
        Ok(())
    }

    struct Projection;

    impl crate::tool::ToolProjection for Projection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(result.value.clone())
        }
    }

    #[derive(Default)]
    struct Journal(
        Mutex<Vec<ExecutionRecord>>,
        Mutex<HashMap<String, (FileRef, Vec<u8>)>>,
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
        ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
            async move {
                self.0
                    .lock()
                    .map(|records| {
                        records
                            .iter()
                            .filter(|record| record.operation_id == operation_id)
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
            _: InteractionId,
            _: Interaction,
        ) -> BoxFuture<'a, Result<()>> {
            async { Err(Error::Unsupported("interactions".into())) }.boxed()
        }

        fn interaction_outcome<'a>(
            &'a self,
            _: InteractionId,
        ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct InterruptedPublisher {
        guarantee: EffectGuarantee,
        observed: bool,
        dispatches: AtomicUsize,
        reconciliations: AtomicUsize,
        admissions: Mutex<Vec<ModelBatchPublication>>,
    }

    impl ModelBatchPublisher for InterruptedPublisher {
        fn identity(&self) -> ComponentIdentity {
            ComponentIdentity {
                name: "test.completed-batch".into(),
                version: "1".into(),
                digest: [27; 32],
            }
        }
        fn guarantee(&self) -> EffectGuarantee {
            self.guarantee
        }
        fn linearizable_reconciliation(&self) -> bool {
            self.guarantee == EffectGuarantee::ExactlyOnce
        }
        fn publish<'a>(&'a self, request: ModelBatchPublication) -> BoxFuture<'a, Result<()>> {
            async move {
                self.admissions.lock().unwrap().push(request);
                if self.dispatches.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(Error::Storage("lost publication response".into()))
                } else {
                    Ok(())
                }
            }
            .boxed()
        }
        fn reconcile<'a>(
            &'a self,
            request: ModelBatchPublication,
        ) -> BoxFuture<'a, Result<Option<()>>> {
            async move {
                self.reconciliations.fetch_add(1, Ordering::SeqCst);
                assert_eq!(self.admissions.lock().unwrap()[0], request);
                Ok(self.observed.then_some(()))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn batch_publication_recovery_preserves_admission_and_retry_guarantee() -> Result<()> {
        for (guarantee, observed, expected_dispatches, success) in [
            (EffectGuarantee::AtMostOnce, true, 1, true),
            (EffectGuarantee::ExactlyOnce, true, 1, true),
            (EffectGuarantee::AtMostOnce, false, 1, false),
            (EffectGuarantee::ExactlyOnce, false, 1, false),
            (EffectGuarantee::IdempotentRetry, false, 2, true),
        ] {
            let publisher = Arc::new(InterruptedPublisher {
                guarantee,
                observed,
                dispatches: AtomicUsize::new(0),
                reconciliations: AtomicUsize::new(0),
                admissions: Mutex::new(Vec::new()),
            });
            let executor = StockExecutor::new(
                Model::new("example", "model", "1", Value::Null)?,
                Arc::new(FakeModel {
                    calls: AtomicUsize::new(0),
                    requests: Mutex::new(Vec::new()),
                }),
                ContextPipeline::default(),
                ToolRegistry::new(),
            )
            .with_batch_publisher(Some(publisher.clone()))?;
            let journal = Journal::default();
            let operation = OperationId::new();
            let request =
                stage_json(&journal, operation, "request", &json!({"request": 1})).await?;
            let boundary =
                stage_json(&journal, operation, "boundary", &json!({"boundary": 1})).await?;
            assert!(matches!(
                executor
                    .publish_completed_batch(
                        &journal,
                        operation,
                        0,
                        request.clone(),
                        boundary.clone()
                    )
                    .await,
                Err(Error::Storage(_))
            ));
            let records = journal.replay(operation).await?;
            assert_eq!(records.len(), 1);
            assert!(matches!(
                records[0].event,
                ExecutionEvent::BatchPublicationStarted { .. }
            ));
            let result = executor
                .publish_completed_batch(&journal, operation, 0, request.clone(), boundary.clone())
                .await;
            if success {
                result?;
                executor
                    .publish_completed_batch(
                        &journal,
                        operation,
                        0,
                        request.clone(),
                        boundary.clone(),
                    )
                    .await?;
                assert_eq!(journal.replay(operation).await?.len(), 2);
            } else {
                assert!(matches!(result, Err(Error::Indeterminate(_))));
                assert_eq!(journal.replay(operation).await?.len(), 1);
            }
            assert_eq!(
                publisher.dispatches.load(Ordering::SeqCst),
                expected_dispatches
            );
            assert_eq!(publisher.reconciliations.load(Ordering::SeqCst), 1);
            let changed =
                stage_json(&journal, operation, "changed", &json!({"boundary": 2})).await?;
            assert!(matches!(
                executor
                    .publish_completed_batch(&journal, operation, 0, request, changed)
                    .await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(
                publisher.dispatches.load(Ordering::SeqCst),
                expected_dispatches
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn completed_batch_publication_blocks_next_request_until_reconciled() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
        let publisher = Arc::new(InterruptedPublisher {
            guarantee: EffectGuarantee::AtMostOnce,
            observed: true,
            dispatches: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
            admissions: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                model_output_schema: json!({"type": "object"}),
            },
            executor: tool.clone(),
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
        )?
        .with_batch_publisher(Some(publisher.clone()))?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("publish".into()),
            selected_context: None,
            max_steps: 2,
        };
        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        let publication = publisher.admissions.lock().unwrap()[0].clone();
        let original: ModelRequest = load_json(&journal, &publication.request).await?;
        let boundary: crate::model_input::CompletedModelBoundary =
            load_json(&journal, &publication.boundary).await?;
        boundary.verify(Limits::default())?;
        assert_eq!(
            &boundary.request.messages[..original.messages.len()],
            original.messages
        );
        assert_eq!(boundary.request.messages.len(), original.messages.len() + 2);
        assert!(
            matches!(&boundary.request.messages[original.messages.len()].content,
            ModelContent::Part(ModelContentPart::ToolCall { call_id, .. }) if call_id == "call-1")
        );
        assert!(
            matches!(&boundary.request.messages[original.messages.len() + 1].content,
            ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) if call_id == "call-1")
        );
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        assert_eq!(model.requests.lock().unwrap()[1], boundary.request);
        executor.execute(input, &journal).await?;
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(publisher.reconciliations.load(Ordering::SeqCst), 1);
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
                model_output_schema: json!({"type": "object"}),
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
    async fn oversized_model_output_is_rejected_before_event_staging() -> Result<()> {
        let model = Arc::new(OversizedOutputModel {
            calls: AtomicUsize::new(0),
        });
        let executor = StockExecutor::new(
            Model::new("example", "oversized", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_limits(Limits {
            file_bytes: 4_096,
            render_bytes: 4_096,
            ..Limits::default()
        });
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([15; 16]),
            input: ModelContent::Text("x".into()),
            selected_context: None,
            max_steps: 1,
        };

        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Invalid(message)) if message == "assistant output exceeds file limit"
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let records = journal.replay(input.operation_id).await?;
        assert!(
            records
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { step: 0, .. }))
        );
        assert!(
            !records
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::Model { step: 0, .. }))
        );
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
                model_output_schema: json!({"type": "object"}),
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
        let first = executor.execute(input.clone(), &journal).await?;
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
        let replayed = executor.execute(input, &journal).await?;
        assert_eq!(first, replayed);
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
                model_output_schema: json!({"type":"object"}),
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
    async fn malformed_tool_arguments_return_to_the_model_instead_of_ending_the_turn() -> Result<()>
    {
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
                model_output_schema: json!({"type": "object"}),
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

        // The turn survives the rejected call and finishes on the corrected one.
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        assert!(
            journal
                .replay(OperationId::from_bytes([79; 16]))
                .await?
                .iter()
                .filter(|record| matches!(
                    record.event,
                    ExecutionEvent::ToolAdmissionRejected {
                        reason: ToolRejectionKind::InvalidArguments,
                        ..
                    }
                ))
                .count()
                == 2
        );

        // The rejected call was never admitted, so only the corrected one reached the executor.
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);

        // The model was told what was wrong, as that call's own tool result.
        let requests = model
            .requests
            .lock()
            .map_err(|_| Error::Storage("model lock poisoned".into()))?;
        let rejections = requests
            .get(1)
            .map(|request| {
                request
                    .messages
                    .iter()
                    .filter_map(|message| match (&message.role, &message.content) {
                        (
                            ModelRole::Tool,
                            ModelContent::Part(ModelContentPart::ToolResult {
                                call_id, value, ..
                            }),
                        ) if call_id == "call-1" || call_id == "call-2" => {
                            Some((call_id.clone(), value.clone()))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .ok_or_else(|| Error::Storage("no rejection request".into()))?;
        assert_eq!(
            rejections
                .iter()
                .map(|(call_id, _)| call_id.as_str())
                .collect::<Vec<_>>(),
            ["call-1", "call-2"]
        );
        for (call_id, rejection) in &rejections {
            let text = rejection
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default();
            assert!(
                text.contains("tool input failed validation"),
                "expected the validation message for {call_id}, got {text:?}"
            );
        }

        // A replay uses the committed completed request and its two durable
        // rejection envelopes; it does not ask the provider to regenerate.
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        let replay = executor.execute(input, &journal).await?;
        assert_eq!(replay.text, "done");
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[tokio::test]
    async fn repeated_rejection_occurrences_survive_completed_batches_and_fresh_replay()
    -> Result<()> {
        struct RepeatedModel {
            calls: AtomicUsize,
            inputs: Mutex<Vec<Vec<u8>>>,
            duplicate_batch: bool,
        }
        impl ModelProvider for RepeatedModel {
            fn generate<'a>(
                &'a self,
                prepared: crate::model_input::PreparedModelInput,
            ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
                self.inputs.lock().unwrap().push(prepared.bytes().to_vec());
                let step = self.calls.fetch_add(1, Ordering::SeqCst);
                let mut events = Vec::new();
                if step < 2 {
                    let call = ModelEvent::ToolCall {
                        call_id: "repeat-call".into(),
                        name: "example.echo".into(),
                        arguments: json!({"unexpected": true}),
                    };
                    events.push(Ok(call.clone()));
                    if self.duplicate_batch {
                        events.push(Ok(call));
                    }
                } else {
                    events.push(Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }));
                }
                events.push(Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }));
                Box::pin(futures::stream::iter(events))
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let make_executor = |provider: Arc<RepeatedModel>| -> Result<StockExecutor> {
            let mut tools = ToolRegistry::new();
            tools.register(crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(), revision: "1".into(), description: "Echo".into(),
                    input_schema: json!({"type":"object", "properties":{"value":{"type":"string"}}, "additionalProperties":false}),
                    output_schema: json!({"type":"object"}),
                    model_output_schema: json!({"type":"object"}),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            })?;
            StockExecutor::new(
                Model::new("example", "model", "1", Value::Null)?,
                provider,
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["tool:call:example.echo"]),
                    Limits::default(),
                )?,
                None,
            )
        };
        let operation = OperationId::from_bytes([81; 16]);
        let invocation = |step| {
            ToolInvocation::for_model_call(
                operation,
                step,
                "repeat-call".into(),
                "example.echo".into(),
                json!({"unexpected":true}),
            )
        };
        assert_ne!(invocation(0).operation_id, invocation(1).operation_id);
        let model = Arc::new(RepeatedModel {
            calls: AtomicUsize::new(0),
            inputs: Mutex::new(Vec::new()),
            duplicate_batch: false,
        });
        let executor = make_executor(model.clone())?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: operation,
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 3,
        };
        assert_eq!(
            executor.execute(input.clone(), &journal).await?.text,
            "done"
        );
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        let inputs = model.inputs.lock().unwrap();
        assert_eq!(inputs.len(), 3);
        let request: ModelRequest = serde_json::from_slice(&inputs[2])
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            request
                .messages
                .iter()
                .map(|message| message.role.as_str())
                .collect::<Vec<_>>(),
            ["user", "assistant", "tool", "assistant", "tool"]
        );
        assert_eq!(request.messages[2].content, request.messages[4].content);
        drop(inputs);
        let records = journal.replay(operation).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.event,
                    ExecutionEvent::ToolAdmissionRejected {
                        reason: ToolRejectionKind::InvalidArguments,
                        ..
                    }
                ))
                .count(),
            2
        );
        let boundary_ref = records
            .iter()
            .find_map(|record| match &record.event {
                ExecutionEvent::ToolBatchCompleted { step: 1, boundary } => Some(boundary),
                _ => None,
            })
            .expect("second completed batch");
        let boundary: crate::model_input::CompletedModelBoundary =
            load_json(&journal, boundary_ref).await?;
        assert_eq!(boundary.rejection_evidence.len(), 2);
        assert_eq!(
            boundary.rejection_evidence[0],
            boundary.rejection_evidence[1]
        );
        boundary.verify(Limits::default())?;
        let fresh = Arc::new(RepeatedModel {
            calls: AtomicUsize::new(0),
            inputs: Mutex::new(Vec::new()),
            duplicate_batch: false,
        });
        assert_eq!(
            make_executor(fresh.clone())?
                .execute(input, &journal)
                .await?
                .text,
            "done"
        );
        assert_eq!(fresh.calls.load(Ordering::SeqCst), 0);
        assert!(fresh.inputs.lock().unwrap().is_empty());
        assert_eq!(records, journal.replay(operation).await?);

        // Boundary publication must reject the same corrupt duplicate
        // preparation history as normal model-step replay.
        let duplicate_preparation = records
            .iter()
            .find_map(|record| match &record.event {
                event @ ExecutionEvent::ModelInputPrepared { step: 1, .. } => Some(event.clone()),
                _ => None,
            })
            .expect("second prepared request");
        journal
            .append(
                operation,
                "duplicate:prepared:step1".into(),
                duplicate_preparation,
            )
            .await?;
        let corrupted_records = journal.replay(operation).await?;
        assert!(
            matches!(executor.record_completed_batch(&journal, operation, 1, &[]).await,
            Err(Error::Storage(message)) if message.contains("duplicate prepared model input"))
        );
        assert_eq!(corrupted_records, journal.replay(operation).await?);

        // Reusing a call ID across steps is valid, but sharing one identity
        // inside a response must fail before admitting any tool operation.
        let duplicate = Arc::new(RepeatedModel {
            calls: AtomicUsize::new(0),
            inputs: Mutex::new(Vec::new()),
            duplicate_batch: true,
        });
        let duplicate_journal = Journal::default();
        let duplicate_operation = OperationId::from_bytes([82; 16]);
        assert!(matches!(make_executor(duplicate)?.execute(TurnInput {
            operation_id: duplicate_operation,
            input: ModelContent::Text("duplicate".into()),
            selected_context: None,
            max_steps: 3,
        }, &duplicate_journal).await,
            Err(Error::Invalid(message)) if message.contains("identity repeated")));
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        assert!(
            !duplicate_journal
                .replay(duplicate_operation)
                .await?
                .iter()
                .any(|record| matches!(
                    record.event,
                    ExecutionEvent::ToolStarted { .. }
                        | ExecutionEvent::ToolCompleted { .. }
                        | ExecutionEvent::ToolAdmissionRejected { .. }
                ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn completed_batch_replay_excludes_future_step_rejection_evidence() -> Result<()> {
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            Arc::new(ReplayModel {
                calls: AtomicUsize::new(0),
            }),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let operation = OperationId::from_bytes([80; 16]);
        let definition = crate::tool::ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "additionalProperties": false,
            }),
            output_schema: json!({"type": "object"}),
            model_output_schema: json!({"type": "object"}),
        };
        let request = ModelRequest {
            model: executor.model.clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: vec![definition.clone()],
            max_output_tokens: None,
        };
        let prepared =
            crate::model_input::PreparedModelInput::prepare(request.clone(), executor.limits)?;
        let manifest =
            stage_json(&journal, operation, "model:0:input", prepared.manifest()).await?;
        let request_file = stage_json(&journal, operation, "model:0:request", &request).await?;
        journal
            .append(
                operation,
                "model:0:input".into(),
                ExecutionEvent::ModelInputPrepared {
                    step: 0,
                    manifest,
                    request: request_file,
                },
            )
            .await?;

        let current = ToolInvocation {
            operation_id: operation,
            call_id: "current-call".into(),
            name: definition.name.clone(),
            arguments: json!({"unexpected": true}),
        };
        let current_error =
            validate_value(&definition.input_schema, &current.arguments, "tool input")
                .expect_err("current call should be malformed")
                .to_string();
        let current_feedback = ToolRejectionFeedback::invalid_arguments(
            &current,
            &definition.input_schema,
            &current_error,
        )?;
        executor
            .record_tool_rejection(
                &journal,
                operation,
                0,
                &current,
                ToolRejectionKind::InvalidArguments,
                Some(&current_feedback),
            )
            .await?;

        let future = ToolInvocation {
            operation_id: operation,
            call_id: "future-call".into(),
            name: definition.name,
            arguments: json!({"unexpected": "future"}),
        };
        let future_error =
            validate_value(&definition.input_schema, &future.arguments, "tool input")
                .expect_err("future call should be malformed")
                .to_string();
        let future_feedback = ToolRejectionFeedback::invalid_arguments(
            &future,
            &definition.input_schema,
            &future_error,
        )?;
        let future_invocation =
            stage_json(&journal, operation, "future:invocation", &future).await?;
        let future_feedback_ref =
            stage_json(&journal, operation, "future:feedback", &future_feedback).await?;
        journal
            .append(
                operation,
                "future:rejection".into(),
                ExecutionEvent::ToolAdmissionRejected {
                    step: 1,
                    invocation: future_invocation,
                    reason: ToolRejectionKind::InvalidArguments,
                    feedback: Some(future_feedback_ref),
                },
            )
            .await?;

        let current_value = current_feedback.to_model_value(&current_error)?;
        executor
            .record_completed_batch(
                &journal,
                operation,
                0,
                &[
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Part(ModelContentPart::ToolCall {
                            call_id: current.call_id.clone(),
                            name: current.name.clone(),
                            arguments: current.arguments.clone(),
                        }),
                    },
                    ModelMessage {
                        role: ModelRole::Tool,
                        content: ModelContent::Part(ModelContentPart::ToolResult {
                            call_id: current.call_id,
                            name: current.name,
                            value: current_value,
                        }),
                    },
                ],
            )
            .await?;
        let boundary_ref = journal
            .replay(operation)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                ExecutionEvent::ToolBatchCompleted { boundary, step: 0 } => Some(boundary),
                _ => None,
            })
            .expect("current batch boundary");
        let boundary: crate::model_input::CompletedModelBoundary =
            load_json(&journal, &boundary_ref).await?;
        assert_eq!(boundary.rejection_evidence, vec![current_feedback]);
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_feedback_bound_to_another_call_identity() -> Result<()> {
        let mut tools = ToolRegistry::new();
        let definition = crate::tool::ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "additionalProperties": false,
            }),
            output_schema: json!({"type": "object"}),
            model_output_schema: json!({"type": "object"}),
        };
        tools.register(crate::tool::Tool {
            definition: definition.clone(),
            executor: Arc::new(FakeTool(AtomicUsize::new(0))),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            Arc::new(ReplayModel {
                calls: AtomicUsize::new(0),
            }),
            ContextPipeline::default(),
            tools,
        );
        let journal = Journal::default();
        let operation = OperationId::from_bytes([84; 16]);
        let invocation = ToolInvocation {
            operation_id: operation,
            call_id: "expected-call".into(),
            name: definition.name.clone(),
            arguments: json!({"unexpected": true}),
        };
        let error = validate_value(
            &definition.input_schema,
            &invocation.arguments,
            "tool input",
        )
        .expect_err("invocation should be malformed")
        .to_string();
        let forged = ToolInvocation {
            call_id: "forged-call".into(),
            ..invocation.clone()
        };
        let forged_feedback =
            ToolRejectionFeedback::invalid_arguments(&forged, &definition.input_schema, &error)?;
        let invocation_ref = stage_json(&journal, operation, "invocation", &invocation).await?;
        let feedback_ref = stage_json(&journal, operation, "feedback", &forged_feedback).await?;
        journal
            .append(
                operation,
                "rejection".into(),
                ExecutionEvent::ToolAdmissionRejected {
                    step: 0,
                    invocation: invocation_ref,
                    reason: ToolRejectionKind::InvalidArguments,
                    feedback: Some(feedback_ref),
                },
            )
            .await?;
        let mut prior = Vec::new();
        assert!(matches!(
            executor
                .resolve_tool_call(&journal, operation, 0, invocation, &mut prior)
                .await,
            Err(Error::Conflict(message)) if message.contains("feedback changed")
        ));
        assert!(prior.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn provider_receives_narrow_projection_and_replay_rejects_stale_projection() -> Result<()>
    {
        let model = Arc::new(ProjectionModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.private".into(),
                revision: "1".into(),
                description: "Private result".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({
                    "type": "object",
                    "required": ["private", "public"],
                    "properties": {
                        "private": {"type": "string"},
                        "public": {"type": "string"},
                    },
                    "additionalProperties": false,
                }),
                model_output_schema: json!({
                    "type": "object",
                    "required": ["public"],
                    "properties": {"public": {"type": "string"}},
                    "additionalProperties": false,
                }),
            },
            executor: Arc::new(PrivateResultTool),
            projection: Arc::new(NarrowProjection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.private"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([81; 16]),
            input: ModelContent::Text("show public result".into()),
            selected_context: None,
            max_steps: 2,
        };

        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        let requests = model
            .requests
            .lock()
            .map_err(|_| Error::Storage("model lock poisoned".into()))?;
        let projected = requests[1]
            .messages
            .iter()
            .find_map(|message| match &message.content {
                ModelContent::Part(ModelContentPart::ToolResult { call_id, value, .. })
                    if call_id == "private-call" =>
                {
                    Some(value.clone())
                }
                _ => None,
            })
            .ok_or_else(|| Error::Storage("provider did not receive projection".into()))?;
        assert_eq!(projected, json!({"public": "shown"}));
        assert!(!projected.to_string().contains("secret"));
        drop(requests);

        // Replacing the pinned projection with another schema-valid value is
        // still rejected: replay derives the projection from the raw result
        // and compares the bytes before allowing the provider request.
        let stale = journal
            .stage(
                input.operation_id,
                "stale-projection".into(),
                crate::contract::canonical_json_bytes(&json!({"public": "stale"}))?,
                "application/json",
            )
            .await?;
        {
            let mut records = journal
                .0
                .lock()
                .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
            for record in &mut *records {
                if let ExecutionEvent::ToolCompleted { projection, .. } = &mut record.event {
                    *projection = stale.clone();
                    break;
                }
            }
        }
        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Conflict(_) | Error::Storage(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_completed_tool_records_fail_before_model_replay() -> Result<()> {
        let model = Arc::new(ProjectionModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.private".into(),
                revision: "1".into(),
                description: "Private result".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object", "required": ["private", "public"], "properties": {"private": {"type": "string"}, "public": {"type": "string"}}, "additionalProperties": false}),
                model_output_schema: json!({"type": "object", "required": ["public"], "properties": {"public": {"type": "string"}}, "additionalProperties": false}),
            },
            executor: Arc::new(PrivateResultTool),
            projection: Arc::new(NarrowProjection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.private"]),
                Limits::default(),
            )?,
            None,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([82; 16]),
            input: ModelContent::Text("show public result".into()),
            selected_context: None,
            max_steps: 2,
        };
        assert_eq!(
            executor.execute(input.clone(), &journal).await?.text,
            "done"
        );
        let completed = journal
            .replay(input.operation_id)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                ExecutionEvent::ToolCompleted { .. } => Some(record.event),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("missing completed tool record".into()))?;
        let mut old_wire = serde_json::to_value(&completed).unwrap();
        old_wire
            .as_object_mut()
            .ok_or_else(|| Error::Storage("completed tool event is not an object".into()))?
            .remove("schema_version");
        let old_event: ExecutionEvent = serde_json::from_value(old_wire).unwrap();
        assert!(matches!(
            old_event.validate_schema_version(),
            Err(Error::Conflict(message)) if message.contains("schema version")
        ));
        journal
            .append(input.operation_id, "duplicate-completed".into(), completed)
            .await?;
        assert!(matches!(
            executor.execute(input, &journal).await,
            Err(Error::Storage(message)) if message.contains("duplicate completed tool result")
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
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
                model_output_schema: json!({"type": "object"}),
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
        let outcome = executor.execute(input.clone(), &journal).await;
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
        let records = journal.replay(input.operation_id).await?;
        assert!(records.iter().any(|record| matches!(
            record.event,
            ExecutionEvent::ToolAdmissionRejected {
                reason: ToolRejectionKind::Unauthorized,
                ..
            }
        )));
        assert!(
            !records
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
        );
        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(records, journal.replay(input.operation_id).await?);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn stock_tool_validation_failure_replays_as_terminal_without_redispatch() -> Result<()> {
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
                output_schema: json!({"type":"string"}),
                model_output_schema: json!({"type":"string"}),
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
            Err(Error::Invalid(message)) if message.contains("pinned schema")));
        assert!(matches!(executor.execute(input, &journal).await,
            Err(Error::Invalid(message)) if message.contains("pinned schema")));
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
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
        let model = Arc::new(RecoverableModel {
            generate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
        });
        let executor = StockExecutor::new(
            Model::new("example", "recoverable", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
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
        let replayed = executor.execute(input, &journal).await?;

        assert_eq!(recovered.text, "partial-restored");
        assert_eq!(replayed, recovered);
        assert_eq!(model.generate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.reconcile_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn fresh_provider_reconcile_receives_persisted_request_bytes_and_manifest() -> Result<()>
    {
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([16; 16]),
            input: ModelContent::Text("restart me".into()),
            selected_context: None,
            max_steps: 1,
        };
        let first_model = Arc::new(PersistedCaptureModel {
            generate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        });
        let first_executor = StockExecutor::new(
            Model::new("example", "restart", "1", Value::Null)?,
            first_model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        assert!(matches!(
            first_executor.execute(input.clone(), &journal).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(first_model.generate_calls.load(Ordering::SeqCst), 1);

        let second_model = Arc::new(PersistedCaptureModel {
            generate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        });
        let second_executor = StockExecutor::new(
            Model::new("example", "restart", "1", Value::Null)?,
            second_model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let recovered = second_executor.execute(input.clone(), &journal).await?;
        assert_eq!(recovered.text, "partial-restored");
        assert_eq!(second_model.generate_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_model.reconcile_calls.load(Ordering::SeqCst), 1);

        let records = journal.replay(input.operation_id).await?;
        let (manifest_ref, request_ref) = records
            .iter()
            .find_map(|record| match &record.event {
                ExecutionEvent::ModelInputPrepared {
                    step: 0,
                    manifest,
                    request,
                } => Some((manifest.clone(), request.clone())),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("persisted model input is missing".into()))?;
        let persisted_bytes = journal.load(&request_ref).await?;
        let persisted_manifest =
            load_json::<crate::model_input::ModelInputManifest>(&journal, &manifest_ref).await?;
        let seen = second_model
            .seen
            .lock()
            .map_err(|_| Error::Storage("capture lock poisoned".into()))?;
        let (reconciled_bytes, reconciled_manifest) = seen
            .first()
            .ok_or_else(|| Error::Storage("reconciliation did not capture input".into()))?;
        assert_eq!(reconciled_bytes, &persisted_bytes);
        assert_eq!(reconciled_manifest, &persisted_manifest);
        assert_eq!(
            *blake3::hash(reconciled_bytes).as_bytes(),
            reconciled_manifest.request_digest
        );
        drop(seen);

        second_executor.execute(input, &journal).await?;
        assert_eq!(second_model.reconcile_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn replay_uses_persisted_model_input_without_rerunning_context() -> Result<()> {
        let model = Arc::new(ReplayModel {
            calls: AtomicUsize::new(0),
        });
        let context_runs = Arc::new(AtomicUsize::new(0));
        let executor = StockExecutor::new(
            Model::new("example", "replay", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::new([Arc::new(CountingContext {
                runs: context_runs.clone(),
            }) as Arc<dyn ContextStage>]),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([11; 16]),
            input: ModelContent::Text("mutable source".into()),
            selected_context: None,
            max_steps: 1,
        };

        executor.execute(input.clone(), &journal).await?;
        assert_eq!(context_runs.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let prepared_count = journal
            .replay(input.operation_id)
            .await?
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    ExecutionEvent::ModelInputPrepared { step: 0, .. }
                )
            })
            .count();
        assert_eq!(prepared_count, 1);

        executor.execute(input, &journal).await?;
        assert_eq!(context_runs.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let records = journal.replay(OperationId::from_bytes([11; 16])).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        ExecutionEvent::ModelInputPrepared { step: 0, .. }
                    )
                })
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn journal_ordering_rejects_unprepared_and_duplicate_model_starts() -> Result<()> {
        let executor = StockExecutor::new(
            Model::new("example", "ordering", "1", Value::Null)?,
            Arc::new(ReplayModel {
                calls: AtomicUsize::new(0),
            }),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let input = TurnInput {
            operation_id: OperationId::from_bytes([14; 16]),
            input: ModelContent::Text("ordering".into()),
            selected_context: None,
            max_steps: 1,
        };

        let unprepared = Journal::default();
        unprepared
            .append(
                input.operation_id,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: executor.request_digest(&input)?,
                },
            )
            .await?;
        unprepared
            .append(
                input.operation_id,
                "model:0:started".into(),
                ExecutionEvent::ModelStarted {
                    step: 0,
                    request_digest: [1; 32],
                },
            )
            .await?;
        assert!(matches!(
            executor.ensure_started(&unprepared, &input).await,
            Err(Error::Storage(message)) if message.contains("missing preparation")
        ));

        let duplicate = Journal::default();
        duplicate
            .append(
                input.operation_id,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: executor.request_digest(&input)?,
                },
            )
            .await?;
        let manifest = duplicate
            .stage(
                input.operation_id,
                "model:0:manifest".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        let request = duplicate
            .stage(
                input.operation_id,
                "model:0:request".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        let prepared = ExecutionEvent::ModelInputPrepared {
            step: 0,
            manifest,
            request,
        };
        duplicate
            .append(
                input.operation_id,
                "model:0:prepared".into(),
                prepared.clone(),
            )
            .await?;
        duplicate
            .append(
                input.operation_id,
                "model:0:prepared-duplicate".into(),
                prepared,
            )
            .await?;
        assert!(matches!(
            executor.ensure_started(&duplicate, &input).await,
            Err(Error::Storage(message)) if message.contains("preparation is duplicated")
        ));

        let rejected = Journal::default();
        rejected
            .append(
                input.operation_id,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: executor.request_digest(&input)?,
                },
            )
            .await?;
        let invocation = rejected
            .stage(
                input.operation_id,
                "tool:invocation".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        rejected
            .append(
                input.operation_id,
                "tool:rejected".into(),
                ExecutionEvent::ToolAdmissionRejected {
                    step: 0,
                    invocation,
                    reason: ToolRejectionKind::InvalidArguments,
                    feedback: None,
                },
            )
            .await?;
        assert!(matches!(
            executor.ensure_started(&rejected, &input).await,
            Err(Error::Storage(message)) if message.contains("missing its admitted model start")
        ));

        let completed = Journal::default();
        completed
            .append(
                input.operation_id,
                "execution:started".into(),
                ExecutionEvent::Started {
                    request_digest: executor.request_digest(&input)?,
                },
            )
            .await?;
        let result = completed
            .stage(
                input.operation_id,
                "tool:result".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        let projection = completed
            .stage(
                input.operation_id,
                "tool:projection".into(),
                b"{}".to_vec(),
                "application/json",
            )
            .await?;
        completed
            .append(
                input.operation_id,
                "tool:completed".into(),
                ExecutionEvent::ToolCompleted {
                    schema_version: TOOL_COMPLETED_EVENT_VERSION,
                    step: 0,
                    call_id: "unstarted-call".into(),
                    invocation_digest: [2; 32],
                    result,
                    projection,
                },
            )
            .await?;
        assert!(matches!(
            executor.ensure_started(&completed, &input).await,
            Err(Error::Storage(message)) if message.contains("completion is missing its admitted start")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_duplicate_or_corrupt_pinned_model_input_before_provider() -> Result<()>
    {
        let model = Arc::new(ReplayModel {
            calls: AtomicUsize::new(0),
        });
        let executor = StockExecutor::new(
            Model::new("example", "replay", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let duplicate_journal = Journal::default();
        let duplicate_input = TurnInput {
            operation_id: OperationId::from_bytes([12; 16]),
            input: ModelContent::Text("duplicate".into()),
            selected_context: None,
            max_steps: 1,
        };
        executor
            .execute(duplicate_input.clone(), &duplicate_journal)
            .await?;
        let prepared = duplicate_journal
            .replay(duplicate_input.operation_id)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                event @ ExecutionEvent::ModelInputPrepared { step: 0, .. } => Some(event),
                _ => None,
            })
            .expect("pinned model input");
        duplicate_journal
            .append(
                duplicate_input.operation_id,
                "model:0:duplicate-input".into(),
                prepared,
            )
            .await?;
        assert!(matches!(
            executor.execute(duplicate_input, &duplicate_journal).await,
            Err(Error::Storage(message)) if message.contains("duplicate prepared model input")
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);

        let corrupt_journal = Journal::default();
        let corrupt_input = TurnInput {
            operation_id: OperationId::from_bytes([13; 16]),
            input: ModelContent::Text("corrupt".into()),
            selected_context: None,
            max_steps: 1,
        };
        executor
            .execute(corrupt_input.clone(), &corrupt_journal)
            .await?;
        let request_ref = corrupt_journal
            .replay(corrupt_input.operation_id)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                ExecutionEvent::ModelInputPrepared {
                    request, step: 0, ..
                } => Some(request),
                _ => None,
            })
            .expect("pinned request");
        let mut stored = corrupt_journal
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
        let entry = stored
            .values_mut()
            .find(|entry| entry.0 == request_ref)
            .ok_or_else(|| Error::NotFound("pinned request bytes".into()))?;
        entry.1 = b"null".to_vec();
        drop(stored);
        assert!(matches!(
            executor.execute(corrupt_input, &corrupt_journal).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_missing_or_corrupt_pinned_manifest_before_provider() -> Result<()> {
        let missing_model = Arc::new(ReplayModel {
            calls: AtomicUsize::new(0),
        });
        let missing_executor = StockExecutor::new(
            Model::new("example", "missing-manifest", "1", Value::Null)?,
            missing_model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let missing_journal = Journal::default();
        let missing_input = TurnInput {
            operation_id: OperationId::from_bytes([17; 16]),
            input: ModelContent::Text("missing manifest".into()),
            selected_context: None,
            max_steps: 1,
        };
        missing_executor
            .execute(missing_input.clone(), &missing_journal)
            .await?;
        let missing_manifest = missing_journal
            .replay(missing_input.operation_id)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                ExecutionEvent::ModelInputPrepared {
                    step: 0, manifest, ..
                } => Some(manifest),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("pinned manifest is missing".into()))?;
        let missing_key = {
            let stored = missing_journal
                .1
                .lock()
                .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
            stored
                .iter()
                .find_map(|(key, (reference, _))| {
                    (reference == &missing_manifest).then(|| key.clone())
                })
                .ok_or_else(|| Error::NotFound("pinned manifest bytes".into()))?
        };
        missing_journal
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .remove(&missing_key);
        assert!(matches!(
            missing_executor
                .execute(missing_input, &missing_journal)
                .await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(missing_model.calls.load(Ordering::SeqCst), 1);

        let corrupt_model = Arc::new(ReplayModel {
            calls: AtomicUsize::new(0),
        });
        let corrupt_executor = StockExecutor::new(
            Model::new("example", "corrupt-manifest", "1", Value::Null)?,
            corrupt_model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let corrupt_journal = Journal::default();
        let corrupt_input = TurnInput {
            operation_id: OperationId::from_bytes([18; 16]),
            input: ModelContent::Text("corrupt manifest".into()),
            selected_context: None,
            max_steps: 1,
        };
        corrupt_executor
            .execute(corrupt_input.clone(), &corrupt_journal)
            .await?;
        let corrupt_manifest = corrupt_journal
            .replay(corrupt_input.operation_id)
            .await?
            .into_iter()
            .find_map(|record| match record.event {
                ExecutionEvent::ModelInputPrepared {
                    step: 0, manifest, ..
                } => Some(manifest),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("pinned manifest is missing".into()))?;
        let mut stored = corrupt_journal
            .1
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
        let entry = stored
            .values_mut()
            .find(|entry| entry.0 == corrupt_manifest)
            .ok_or_else(|| Error::NotFound("pinned manifest bytes".into()))?;
        entry.1 = b"null".to_vec();
        drop(stored);
        assert!(matches!(
            corrupt_executor
                .execute(corrupt_input, &corrupt_journal)
                .await,
            Err(Error::Storage(_))
        ));
        assert_eq!(corrupt_model.calls.load(Ordering::SeqCst), 1);
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
        let replayed_first = journal.replay(first).await?;
        let [first_entry] = replayed_first.as_slice() else {
            unreachable!("expected exactly one replayed event for the first operation");
        };
        assert_eq!(first_entry.sequence, 1);
        let replayed_second = journal.replay(second).await?;
        let [second_entry] = replayed_second.as_slice() else {
            unreachable!("expected exactly one replayed event for the second operation");
        };
        assert_eq!(second_entry.sequence, 1);
        Ok(())
    }
}
