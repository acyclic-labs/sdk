//! Fully replaceable turn execution and the stock streaming model/tool loop.

use crate::{
    Error, InteractionId, OperationId, Result,
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
        RuntimeScope, ToolPolicy, ToolPolicyDecision, check_tool_approval, validate_policy_identity,
    },
    tool::{ToolInvocation, ToolRegistry, ToolResult, validate_value},
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
            provider,
            context,
            tools,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
        }
    }

    /// Applies the composition's checked bounds to the stock loop.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
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

    fn request_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest(&json!({
            "executor": "acyclic.stock.v2",
            "input": input,
            "model": self.model,
            "context": self.context.contracts(),
            "tools": self.tools.definitions()?,
            "limits": self.limits,
            "tool_scope": (self.tool_scope.grants(), self.tool_scope.limits()),
            "policy": self.policy_identity.as_ref(),
        }))
    }

    /// Replays the durable journal for one turn, verifying it is gapless and bound to the
    /// exact same request, and journals the initial `Started` marker on a fresh turn.
    async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        let records = journal.replay(input.operation_id).await?;
        for (index, record) in records.iter().enumerate() {
            if record.operation_id != input.operation_id || record.sequence != index as u64 + 1 {
                return Err(Error::Conflict(
                    "execution journal is not gapless or belongs to another turn".into(),
                ));
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
    ) -> Result<Vec<ModelEvent>> {
        let records = journal.replay(input.operation_id).await?;
        let context = self
            .context
            .run(&ContextInput {
                input: input.input.clone(),
                selected_context: input.selected_context.clone(),
                step,
                prior_messages: prior_messages.to_vec(),
            })
            .await?;
        let mut replayed_model = Vec::new();
        let mut admission = ModelEventAdmission::default();
        for record in &records {
            if let ExecutionEvent::Model {
                step: event_step,
                event,
            } = &record.event
                && *event_step == step
            {
                let event = load_json::<ModelEvent>(journal, event).await?;
                admission.observe(&event, self.limits)?;
                replayed_model.push(event);
            }
        }
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
                        .contains(&format!("tool:call:{}", tool.name))
                })
                .collect(),
            max_output_tokens: None,
        };
        let request_digest = model_request_digest(&request)?;
        let started = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                step: event_step,
                request_digest,
            } if *event_step == step => Some(*request_digest),
            _ => None,
        });
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
            let current = journal.replay(input.operation_id).await?;
            if let Some(existing) = current.iter().find_map(|record| match &record.event {
                ExecutionEvent::ModelStarted {
                    step: event_step,
                    request_digest,
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
            let mut stream = self.provider.generate(request);
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
        Ok(model_events)
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
    ) -> Result<()> {
        let records = journal.replay(operation_id).await?;
        invocation.validate()?;
        let tool = self
            .tools
            .get(&invocation.name)
            .ok_or_else(|| Error::NotFound(format!("tool {}", invocation.name)))?;
        validate_value(
            &tool.definition.input_schema,
            &invocation.arguments,
            "tool input",
        )?;
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
        let (result, projection) = if let Some(completed) = completed_tool {
            (
                load_json::<ToolResult>(journal, &completed.0).await?,
                load_json::<Value>(journal, &completed.1).await?,
            )
        } else {
            {
                let scope = &self.tool_scope;
                let capability = format!("tool:call:{}", tool.definition.name);
                if !scope.grants().contains(&capability) {
                    return Err(Error::Unauthorized(format!("scope lacks {capability}")));
                }
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
            let started = records.iter().find_map(|record| match &record.event {
                ExecutionEvent::ToolStarted {
                    step: event_step,
                    call_id,
                    invocation: existing,
                } if *event_step == step && call_id == &invocation.call_id => Some(existing),
                _ => None,
            });
            let claimed = if let Some(existing) = started {
                if load_json::<ToolInvocation>(journal, existing).await? != invocation {
                    return Err(Error::Conflict(
                        "tool call identity is bound to another invocation".into(),
                    ));
                }
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
            let result = if claimed {
                match tool.executor.execute(invocation.clone()).await {
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
                match tool.executor.reconcile(invocation.clone()).await {
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
            let projection = match tool.projection.project(&invocation, &result) {
                Ok(projection) => projection,
                Err(_) => {
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
                }
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
        prior_messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: invocation.call_id.clone(),
                name: invocation.name.clone(),
                value: projection,
            }),
        });
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
                for message in &selected.messages {
                    message.content.validate_limits(self.limits)?;
                    for reference in message.content.file_refs() {
                        journal.verify_input_file(reference).await?;
                    }
                }
            }
            for reference in input.input.file_refs() {
                journal.verify_input_file(reference).await?;
            }
            self.ensure_started(journal, &input).await?;
            let mut prior_messages = Vec::new();
            let mut text = String::new();
            for step in 0..input.max_steps {
                let mut calls = Vec::new();
                let mut completed = None;
                let model_events = self
                    .run_model_step(journal, &input, step, &prior_messages)
                    .await?;
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
                            calls.push(ToolInvocation {
                                call_id,
                                name,
                                arguments,
                            });
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
                        text,
                        attachments: Vec::new(),
                        metadata,
                        steps: step + 1,
                    });
                }
                for invocation in calls {
                    prior_messages.push(ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Part(ModelContentPart::ToolCall {
                            call_id: invocation.call_id.clone(),
                            name: invocation.name.clone(),
                            arguments: invocation.arguments.clone(),
                        }),
                    });
                    self.resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        &mut prior_messages,
                    )
                    .await?;
                }
            }
            Err(Error::Conflict("executor step limit reached".into()))
        })
    }
}

fn model_request_digest(request: &ModelRequest) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(request)
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
    reference.descriptor().verify(&bytes)?;
    let parsed: Value = serde_json::from_slice(&bytes)
        .map_err(|error| Error::Storage(format!("execution journal JSON is invalid: {error}")))?;
    if crate::contract::canonical_json_bytes(&parsed)
        .map_err(|error| Error::Storage(error.to_string()))?
        != bytes
    {
        return Err(Error::Storage(
            "execution journal JSON is not canonical".into(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| Error::Storage(format!("execution journal content is invalid: {error}")))
}

#[derive(Default)]
struct ModelEventAdmission {
    count: usize,
    calls: BTreeSet<String>,
    completed: bool,
}

impl ModelEventAdmission {
    fn observe(&mut self, event: &ModelEvent, limits: Limits) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        conversation::{FileDescriptor, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use futures::{FutureExt as _, stream};
    use std::collections::HashMap;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct FakeModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for FakeModel {
        fn generate<'a>(
            &'a self,
            request: ModelRequest,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
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
    }

    impl ModelProvider for RecoverableModel {
        fn generate<'a>(
            &'a self,
            _: ModelRequest,
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
        let mut narrow = Limits::default();
        narrow.model_steps = 1;
        assert!(matches!(
            base.clone()
                .with_limits(narrow)
                .execute(input.clone(), &journal)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        let _ = base.execute(input.clone(), &journal).await?;
        let mut changed = Limits::default();
        changed.model_steps = 3;
        assert!(matches!(
            base.with_limits(changed).execute(input, &journal).await,
            Err(Error::Conflict(_))
        ));
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
