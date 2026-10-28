//! Main reducer records and their admission metadata.
use super::*;
/// Fixed aggregate lifecycle shared by every durable handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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

impl LifecycleState {}

/// Schema-validated event payload retained in canonical history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
        record: Box<ExtensionRecord>,
    },
    /// Explicit transition between exact retained extension state versions.
    ExtensionStateMigrated {
        /// Exact validated source and target revision pair.
        migration: Box<ExtensionStateMigration>,
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
        previous: Option<Box<ExtensionConfiguration>>,
        /// Immutable configuration revision.
        record: Box<ExtensionConfiguration>,
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
        request: Box<FileRef>,
        /// Canonical digest of the complete provider request identity.
        request_digest: [u8; 32],
        /// Pinned JSON Schema file for a successful provider result.
        result_schema: Box<FileRef>,
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
        observation: Box<EffectAttestation>,
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
        selection: Box<ModelContextSelection>,
    },
    /// A ref-only interaction became addressable to one granted responder.
    InteractionOpened {
        /// Admitted request identity and ref.
        ticket: Box<InteractionTicket>,
    },
    /// A responder closed one exact open interaction version.
    InteractionResolved {
        /// Exact responder outcome.
        resolution: Box<InteractionResolution>,
    },
}

/// Reference to an exact event in another aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct EventReference {
    /// Referenced authority.
    pub authority: Authority,
    /// Exact referenced revision.
    pub revision: u64,
}

/// Non-bearer admission metadata retained in durable history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct RecordedScope {
    pub(in crate::core) id: String,
    pub(in crate::core) capabilities: Capabilities,
    pub(in crate::core) issuer: String,
    pub(in crate::core) agent: Option<AgentId>,
}

impl RecordedScope {
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
}

/// One canonical event in an aggregate history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
    /// Sealed Ed25519 workload descendant and runtime receipt for this event.
    #[serde(with = "super::capability_bytes")]
    #[heap_layout(as = crate::heap_layout::markers::Base64Bytes)]
    pub attestation: Vec<u8>,
    /// Optional causal predecessor in another aggregate.
    pub causal_parent: Option<EventReference>,
    /// Typed payload.
    pub payload: EventPayload,
}

impl Event {}

/// Immutable command envelope accepted by the reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
        content: Box<FileRef>,
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
        content: Box<FileRef>,
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
        content: Box<FileRef>,
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
        request: Box<FileRef>,
        /// Pinned JSON Schema file for a successful provider result.
        result_schema: Box<FileRef>,
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
        observation: Box<EffectAttestation>,
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
        selection: Box<ModelContextSelection>,
    },
    /// Admits a validated staged request without embedding its prompt or schema.
    OpenInteraction {
        /// Request identity and staged content ref.
        ticket: Box<InteractionTicket>,
    },
    /// Resolves an open request with a responder-specific signed capability.
    ResolveInteraction {
        /// Expected version and explicit outcome.
        resolution: Box<InteractionResolution>,
    },
}

/// Result of applying a command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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

// Action and event wire tags share one transition table.
macro_rules! transitions {
    ($($action:pat => $event:pat => $action_tag:literal, $event_tag:literal;)*) => {
        impl Action {
            /// Serde action tag carried beside canonical JSON.
            pub(crate) fn tag(&self) -> &'static str {
                match self {
                    $($action => $action_tag,)*
                }
            }
        }
        impl EventPayload {
            /// Serde payload tag carried beside canonical JSON.
            pub(crate) fn tag(&self) -> &'static str {
                match self {
                    $($event => $event_tag,)*
                }
            }
        }
    };
}

transitions! {
    Self::TransitionLifecycle { .. } => Self::LifecycleTransitioned { .. } =>
        "transition_lifecycle", "lifecycle_transitioned";
    Self::AppendCustom { .. } => Self::Custom { .. } =>
        "append_custom", "custom";
    Self::MigrateExtensionState { .. } => Self::ExtensionStateMigrated { .. } =>
        "migrate_extension_state", "extension_state_migrated";
    Self::SelectExtensions { .. } => Self::ExtensionsSelected { .. } =>
        "select_extensions", "extensions_selected";
    Self::ConfigureExtension { .. } => Self::ExtensionConfigured { .. } =>
        "configure_extension", "extension_configured";
    Self::PlanEffect { .. } => Self::EffectPlanned { .. } =>
        "plan_effect", "effect_planned";
    Self::MarkEffectDispatched { .. } => Self::EffectDispatched { .. } =>
        "mark_effect_dispatched", "effect_dispatched";
    Self::ResolveEffect { .. } => Self::EffectResolved { .. } =>
        "resolve_effect", "effect_resolved";
    Self::BindConversation { .. } => Self::ConversationBound { .. } =>
        "bind_conversation", "conversation_bound";
    Self::AppendConversationMessage { .. } => Self::ConversationMessageAppended { .. } =>
        "append_conversation_message", "conversation_message_appended";
    Self::SelectModelContext { .. } => Self::ModelContextSelected { .. } =>
        "select_model_context", "model_context_selected";
    Self::OpenInteraction { .. } => Self::InteractionOpened { .. } =>
        "open_interaction", "interaction_opened";
    Self::ResolveInteraction { .. } => Self::InteractionResolved { .. } =>
        "resolve_interaction", "interaction_resolved";
}

crate::core::ownership::owned_enum!(LifecycleState {
    Pending,
    Active,
    Waiting,
    Completed,
    Failed,
    Cancelled,
});
crate::core::ownership::owned_enum!(EventPayload {
    LifecycleTransitioned { from, to, reason },
    Custom { record },
    ExtensionStateMigrated { migration },
    ExtensionsSelected { previous, selected, configurations },
    ExtensionConfigured { previous, record },
    EffectPlanned { effect_id, provider, guarantee, effect_kind, request, request_digest, result_schema, result_schema_digest },
    EffectDispatched { effect_id, attempt_id },
    EffectResolved { observation },
    ConversationBound { agent },
    ConversationMessageAppended { message },
    ModelContextSelected { selection },
    InteractionOpened { ticket },
    InteractionResolved { resolution },
});
crate::core::ownership::owned_struct!(EventReference {
    authority,
    revision
});
crate::core::ownership::owned_struct!(RecordedScope {
    id,
    capabilities,
    issuer,
    agent
});
crate::core::ownership::owned_struct!(Event {
    revision,
    operation_id,
    intent_digest,
    scope,
    attestation,
    causal_parent,
    payload
});
crate::core::ownership::owned_struct!(Command {
    operation_id,
    idempotency_key,
    expected_revision,
    scope,
    causal_parent,
    action
});
crate::core::ownership::owned_enum!(Action {
    TransitionLifecycle { to, reason },
    AppendCustom { schema, version, content },
    MigrateExtensionState { name, previous, to_version, content },
    SelectExtensions { roots },
    ConfigureExtension { extension, content },
    PlanEffect { effect_id, provider, guarantee, effect_kind, request, result_schema },
    MarkEffectDispatched { effect_id, attempt_id },
    ResolveEffect { observation },
    BindConversation { agent },
    AppendConversationMessage { message },
    SelectModelContext { selection },
    OpenInteraction { ticket },
    ResolveInteraction { resolution },
});
