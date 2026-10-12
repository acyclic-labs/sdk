//! Main reducer records and their admission metadata.
use super::*;
/// Fixed aggregate lifecycle shared by every durable handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// Aggregate exists but has not begun work.
    Pending,
    /// Aggregate is actively executing or accepting input.
    Active,
    /// Aggregate is durably suspended for external input or children.
    Waiting,
    /// Aggregate completed successfully.
    Completed,
    /// Aggregate terminated with failure.
    Failed,
    /// Aggregate acknowledged cancellation.
    Cancelled,
}

impl LifecycleState {
    pub(in crate::core) fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Schema-validated event payload retained in canonical history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "canonical events prioritize typed fields over enum stack size"
)]
pub enum EventPayload {
    /// Fixed aggregate lifecycle transition.
    LifecycleTransitioned {
        /// Previous state.
        from: LifecycleState,
        /// New state.
        to: LifecycleState,
        /// Optional stable diagnostic.
        reason: Option<String>,
    },
    /// Namespaced extension event validated by its registered schema.
    Custom {
        /// Complete typed and version-pinned extension record.
        record: ExtensionRecord,
    },
    /// Explicit transition between exact retained extension state versions.
    ExtensionStateMigrated {
        /// Exact validated source and target revision pair.
        migration: ExtensionStateMigration,
    },
    /// Agent-owned active extension selection, including its resolved dependencies.
    ExtensionsSelected {
        /// Exact predecessor needed for deterministic replay.
        previous: Vec<ExtensionDependency>,
        /// Dependency-first selected versions for new admissions only.
        selected: Vec<ExtensionDependency>,
        /// Exact validated configuration revisions pinned for this selection.
        configurations: Vec<ExtensionConfiguration>,
    },
    /// Agent-owned, provider-validated configuration publication.
    ExtensionConfigured {
        /// Exact predecessor for replay and concurrent update detection.
        previous: Option<ExtensionConfiguration>,
        /// Immutable configuration revision.
        record: ExtensionConfiguration,
    },
    /// Durable intent to perform one external effect.
    EffectPlanned {
        /// Stable effect identity.
        effect_id: EffectId,
        /// Stable provider identity.
        provider: String,
        /// Provider-advertised delivery strength.
        guarantee: EffectGuarantee,
        /// Namespaced effect kind.
        effect_kind: String,
        /// Pinned provider request file.
        request: FileRef,
        /// Canonical digest of the complete provider request identity.
        request_digest: [u8; 32],
        /// Pinned JSON Schema file for a successful provider result.
        result_schema: FileRef,
        /// Digest pinning the result schema.
        result_schema_digest: [u8; 32],
    },
    /// Durable effect lifecycle transition.
    EffectDispatched {
        /// Stable effect identity.
        effect_id: EffectId,
        /// Unique identity for this provider attempt.
        attempt_id: EffectAttemptId,
    },
    /// One provider attempt produced a semantic observation.
    EffectResolved {
        /// Complete host-attested provider observation.
        observation: EffectAttestation,
    },
    /// Prepared immutable environment references became atomically visible.
    ForkPublished {
        /// Complete child publication record.
        seed: Box<ForkSeed>,
    },
    /// An inspected project join and its parent-conversation notice became visible.
    ProjectMergePublished {
        /// Exact provider result, bound to a previously published child fork.
        receipt: Box<ProjectMergeReceipt>,
    },
    /// An agent acquired permanent ownership of this conversation.
    ConversationBound {
        /// Owning agent.
        agent: crate::AgentId,
    },
    /// One ordered, ref-only conversation message was admitted.
    ConversationMessageAppended {
        /// Canonical record.
        message: Box<ConversationMessage>,
    },
    /// An exact ordered history subset was selected for a model request.
    ModelContextSelected {
        /// Revision-pinned source identities; projected bytes remain outside history.
        selection: ModelContextSelection,
    },
    /// A ref-only interaction became addressable to one granted responder.
    InteractionOpened {
        /// Admitted request identity and ref.
        ticket: InteractionTicket,
    },
    /// A responder closed one exact open interaction version.
    InteractionResolved {
        /// Exact responder outcome.
        resolution: InteractionResolution,
    },
}

/// Reference to an exact event in another aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventReference {
    /// Referenced authority.
    pub authority: Authority,
    /// Exact referenced revision.
    pub revision: u64,
}

/// Non-bearer admission metadata retained in durable history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedScope {
    pub(in crate::core) id: String,
    pub(in crate::core) capabilities: Capabilities,
    pub(in crate::core) issuer: String,
    pub(in crate::core) agent: Option<AgentId>,
}

impl RecordedScope {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let capabilities = self.capabilities.logical_owned_bytes()?;
        (self.id.capacity() as u64)
            .checked_add(self.issuer.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(capabilities))
            .ok_or_else(|| Error::Invalid("recorded scope ownership overflows".into()))
    }

    pub(in crate::core) fn from_scope(scope: &Scope) -> Self {
        Self {
            id: scope.id.clone(),
            capabilities: scope.capabilities.clone(),
            issuer: scope.issuer.clone(),
            agent: scope.agent,
        }
    }

    /// Capabilities authenticated for this one committed event.
    #[must_use]
    pub const fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Stable admission-scope label retained for audit only.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Host-attested actor, if the admission was agent-bound.
    #[must_use]
    pub const fn agent(&self) -> Option<AgentId> {
        self.agent
    }

    pub(crate) fn wire_parts(&self) -> (&str, Option<AgentId>) {
        (&self.issuer, self.agent)
    }

    pub(crate) fn from_wire(
        id: String,
        capabilities: Capabilities,
        issuer: String,
        agent: Option<AgentId>,
    ) -> Self {
        Self {
            id,
            capabilities,
            issuer,
            agent,
        }
    }
}

/// One canonical event in an aggregate history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Gapless one-based aggregate revision.
    pub revision: u64,
    /// Operation that caused this event.
    pub operation_id: OperationId,
    /// Digest that permanently binds the operation identity to its command.
    pub intent_digest: [u8; 32],
    /// Non-transferable admission metadata; never a reusable authorization scope.
    pub scope: RecordedScope,
    /// Keyed proof over this complete event, including its admission metadata.
    pub attestation: [u8; 32],
    /// Optional causal predecessor in another aggregate.
    pub causal_parent: Option<EventReference>,
    /// Typed payload.
    pub payload: EventPayload,
}

impl Event {
    pub(in crate::core) fn replay(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if let Some(io) = work.as_deref_mut().filter(|io| !copying::is_native(&**io))
            && matches!(&self.payload, EventPayload::ForkPublished { .. })
        {
            io.admit_allocation(std::mem::size_of::<ForkSeed>() as u64)?;
        }
        let (event, retained) =
            crate::wire_codec::CodecQuota::produce(work, |quota| copying::copy_event(self, quota))?;
        Ok((ApplyResult::Replayed { event }, retained))
    }
}

/// Immutable command envelope accepted by the reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    /// Stable identity allocated before admission.
    pub operation_id: OperationId,
    /// Caller-owned retry identity.
    pub idempotency_key: IdempotencyKey,
    /// Required current aggregate revision.
    pub expected_revision: u64,
    /// Explicit authority captured for this invocation.
    pub scope: Scope,
    /// Optional exact causal predecessor.
    pub causal_parent: Option<EventReference>,
    /// Requested deterministic transition.
    pub action: Action,
}

/// Transitions supported by the substrate reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Advances the fixed aggregate lifecycle.
    TransitionLifecycle {
        /// Requested next state.
        to: LifecycleState,
        /// Optional stable diagnostic.
        reason: Option<String>,
    },
    /// Appends one registered extension event.
    AppendCustom {
        /// Namespaced schema identity.
        schema: String,
        /// Positive schema version.
        version: u32,
        /// Staged, version-pinned JSON payload.
        content: FileRef,
    },
    /// Publishes a validated state revision under another installed version.
    MigrateExtensionState {
        /// Namespaced state identity; the latest revision must match `previous`.
        name: String,
        /// Exact migration input event in this aggregate.
        previous: EventReference,
        /// Installed target schema and implementation version.
        to_version: u32,
        /// Staged target JSON, validated before event publication.
        content: FileRef,
    },
    /// Selects installed extension versions for future agent admissions.
    SelectExtensions {
        /// Exact roots; dependencies are resolved from pinned registrations.
        roots: Vec<ExtensionDependency>,
    },
    /// Publishes a staged configuration for one installed extension version.
    ConfigureExtension {
        /// Installed extension version.
        extension: ExtensionDependency,
        /// Owner-verified JSON file; bytes never enter the event.
        content: FileRef,
    },
    /// Records intent for one external effect.
    PlanEffect {
        /// Stable effect identity.
        effect_id: EffectId,
        /// Stable selected provider identity.
        provider: String,
        /// Provider-advertised delivery strength.
        guarantee: EffectGuarantee,
        /// Namespaced effect kind.
        effect_kind: String,
        /// Pinned provider request file.
        request: FileRef,
        /// Pinned JSON Schema file for a successful provider result.
        result_schema: FileRef,
    },
    /// Records that effect dispatch began.
    MarkEffectDispatched {
        /// Stable effect identity.
        effect_id: EffectId,
        /// Unique identity allocated before calling the provider.
        attempt_id: EffectAttemptId,
    },
    /// Records one semantic effect completion.
    ResolveEffect {
        /// Provider observation attested by the aggregate authority host.
        observation: EffectAttestation,
    },
    /// Atomically publishes one fully prepared child environment.
    PublishFork {
        /// Immutable references prepared outside the reducer.
        seed: Box<ForkSeed>,
    },
    /// Records an already-applied parent-controlled project join.
    PublishProjectMerge {
        /// Provider-verified merge result and ref-only notice.
        receipt: Box<ProjectMergeReceipt>,
    },
    /// Binds an empty conversation to an agent exactly once.
    BindConversation {
        /// Owning agent.
        agent: crate::AgentId,
    },
    /// Appends a version-pinned, payload-free message.
    AppendConversationMessage {
        /// Canonical record.
        message: Box<ConversationMessage>,
    },
    /// Records a deliberate context selection separately from canonical messages.
    SelectModelContext {
        /// Exact ordered conversation subset.
        selection: ModelContextSelection,
    },
    /// Admits a validated staged request without embedding its prompt or schema.
    OpenInteraction {
        /// Request identity and staged content ref.
        ticket: InteractionTicket,
    },
    /// Resolves an open request with a responder-specific signed capability.
    ResolveInteraction {
        /// Expected version and explicit outcome.
        resolution: InteractionResolution,
    },
}

/// Result of applying a command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ApplyResult {
    /// A new event was committed to reducer state.
    Applied {
        /// Newly committed event.
        event: Event,
    },
    /// The exact operation was already committed.
    Replayed {
        /// Previously committed event.
        event: Event,
    },
}

// Keep wire tags and authorization in the same transition table.
macro_rules! transitions {
    ($($action:pat => $event:pat => $action_tag:literal, $event_tag:literal,
        $capability:ident, $subject:expr;)*) => {
        impl Action {
            pub(in crate::core) fn admission(&self) -> (&'static str, &'static str, Subject<'_>) {
                match self {
                    $($action => ($action_tag, capability::$capability, $subject),)*
                }
            }
            /// Serde action tag carried beside canonical JSON.
            pub(crate) fn tag(&self) -> &'static str {
                self.admission().0
            }
        }
        impl EventPayload {
            pub(in crate::core) fn admission(&self) -> (&'static str, &'static str, Subject<'_>) {
                match self {
                    $($event => ($event_tag, capability::$capability, $subject),)*
                }
            }
            /// Serde payload tag carried beside canonical JSON.
            pub(crate) fn tag(&self) -> &'static str {
                self.admission().0
            }
        }
    };
}

transitions! {
    Self::TransitionLifecycle { .. } => Self::LifecycleTransitioned { .. } =>
        "transition_lifecycle", "lifecycle_transitioned", LIFECYCLE_MANAGE, Subject::None;
    Self::AppendCustom { .. } => Self::Custom { .. } =>
        "append_custom", "custom", EVENT_APPEND, Subject::None;
    Self::MigrateExtensionState { .. } => Self::ExtensionStateMigrated { .. } =>
        "migrate_extension_state", "extension_state_migrated", EXTENSION_MIGRATE, Subject::None;
    Self::SelectExtensions { .. } => Self::ExtensionsSelected { .. } =>
        "select_extensions", "extensions_selected", EXTENSION_ACTIVATE, Subject::None;
    Self::ConfigureExtension { .. } => Self::ExtensionConfigured { .. } =>
        "configure_extension", "extension_configured", EXTENSION_CONFIGURE, Subject::None;
    Self::PlanEffect { .. } => Self::EffectPlanned { .. } =>
        "plan_effect", "effect_planned", EFFECT_RUN, Subject::PlannedEffect;
    Self::MarkEffectDispatched { effect_id, .. } => Self::EffectDispatched { effect_id, .. } =>
        "mark_effect_dispatched", "effect_dispatched", EFFECT_RUN, Subject::Effect(effect_id);
    Self::ResolveEffect { observation } => Self::EffectResolved { observation } =>
        "resolve_effect", "effect_resolved", EFFECT_RUN, Subject::Effect(&observation.effect_id);
    Self::PublishFork { seed } => Self::ForkPublished { seed } =>
        "publish_fork", "fork_published", FORK_PUBLISH, Subject::Fork(seed);
    Self::PublishProjectMerge { receipt } => Self::ProjectMergePublished { receipt } =>
        "publish_project_merge", "project_merge_published", PROJECT_MERGE, Subject::Merge(receipt);
    Self::BindConversation { .. } => Self::ConversationBound { .. } =>
        "bind_conversation", "conversation_bound", CONVERSATION_BIND, Subject::None;
    Self::AppendConversationMessage { .. } => Self::ConversationMessageAppended { .. } =>
        "append_conversation_message", "conversation_message_appended", CONVERSATION_APPEND, Subject::None;
    Self::SelectModelContext { .. } => Self::ModelContextSelected { .. } =>
        "select_model_context", "model_context_selected", CONVERSATION_SELECT_CONTEXT, Subject::None;
    Self::OpenInteraction { .. } => Self::InteractionOpened { .. } =>
        "open_interaction", "interaction_opened", INTERACTION_OPEN, Subject::None;
    Self::ResolveInteraction { resolution } => Self::InteractionResolved { resolution } =>
        "resolve_interaction", "interaction_resolved", INTERACTION_RESOLVE, Subject::Interaction(resolution);
}

/// Transition input whose authorization depends on reducer state.
pub(in crate::core) enum Subject<'a> {
    None,
    PlannedEffect,
    Effect(&'a EffectId),
    Interaction(&'a InteractionResolution),
    Fork(&'a ForkSeed),
    Merge(&'a ProjectMergeReceipt),
}

pub(in crate::core) fn validate_interaction_transition(
    ticket: &InteractionTicket,
    prior: Option<&InteractionResolution>,
    resolution: &InteractionResolution,
) -> Result<()> {
    resolution.validate(ticket)?;
    if prior.is_some_and(|value| value.outcome.is_terminal()) {
        return Err(Error::Conflict("interaction is already resolved".into()));
    }
    let expected = prior.map_or(Ok(1_u64), |value| next_revision(value.expected_version))?;
    if resolution.expected_version != expected {
        return Err(Error::Conflict(
            "interaction resolution version mismatch".into(),
        ));
    }
    Ok(())
}

pub(in crate::core) fn validate_interaction_operation(
    resolution: &InteractionResolution,
    operation_id: OperationId,
) -> Result<()> {
    if matches!(&resolution.outcome,
        InteractionOutcome::Indeterminate { operation_id: pending } if *pending != operation_id)
    {
        return Err(Error::Conflict(
            "indeterminate interaction names another operation".into(),
        ));
    }
    Ok(())
}

pub(in crate::core) fn require_capability(
    (id, capabilities): (&str, &Capabilities),
    capability: &str,
) -> Result<()> {
    if capabilities.contains(capability) {
        Ok(())
    } else {
        Err(Error::Unauthorized(format!(
            "scope {id} lacks capability {capability}"
        )))
    }
}

pub(in crate::core) fn validate_causal_parent_with_work(
    authority: &Authority,
    current_revision: u64,
    reference: Option<&EventReference>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let Some(reference) = reference else {
        return Ok(());
    };
    if reference.revision == 0 {
        return Err(crate::fork::boundary_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "causal event revision must be positive",
        ));
    }
    if reference.authority == *authority && reference.revision > current_revision {
        return Err(crate::fork::boundary_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "causal event is in the future",
        ));
    }
    reference.authority.validate_with_work(work)?;
    Ok(())
}

pub(in crate::core) fn validate_lifecycle(from: LifecycleState, to: LifecycleState) -> Result<()> {
    let valid = matches!(
        (from, to),
        (LifecycleState::Pending, LifecycleState::Active)
            | (LifecycleState::Pending, LifecycleState::Cancelled)
            | (LifecycleState::Active, LifecycleState::Waiting)
            | (LifecycleState::Active, LifecycleState::Completed)
            | (LifecycleState::Active, LifecycleState::Failed)
            | (LifecycleState::Active, LifecycleState::Cancelled)
            | (LifecycleState::Waiting, LifecycleState::Active)
            | (LifecycleState::Waiting, LifecycleState::Completed)
            | (LifecycleState::Waiting, LifecycleState::Failed)
            | (LifecycleState::Waiting, LifecycleState::Cancelled)
    );
    if valid {
        Ok(())
    } else if from.is_terminal() {
        Err(Error::Conflict("terminal lifecycle cannot advance".into()))
    } else {
        Err(Error::Conflict("invalid lifecycle transition".into()))
    }
}
