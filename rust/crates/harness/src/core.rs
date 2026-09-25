//! Deterministic, host-neutral durable substrate semantics.

use crate::{
    AgentId, Capabilities, EffectAttemptId, EffectId, Error, IdempotencyKey, OperationId,
    PolicyLayer, Result,
    conversation::{
        ConversationMessage, ConversationState, FileDescriptor, FileRef, ModelContextSelection,
        VolumeClass, VolumeOperation, VolumeOwner,
    },
    fork::{ForkSeed, InheritedConversationPrefix},
    interaction::{InteractionResolution, InteractionTicket},
    merge::ProjectMergeReceipt,
    resolve_policy_layers,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Kind of independently ordered durable aggregate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind {
    /// Durable agent definition and configuration.
    Agent,
    /// Durable user-visible conversation.
    Conversation,
    /// Durable execution session within a conversation.
    Session,
    /// Durable user-driven turn.
    Turn,
    /// Durable general-purpose task.
    Task,
}

/// Stable identity of one independently ordered history.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    /// Aggregate kind.
    pub kind: AggregateKind,
    /// Provider-independent identity.
    pub id: String,
}

impl Authority {
    /// Returns the only Stream path that may own this aggregate.
    pub fn stream_path(&self) -> Result<String> {
        if self.id.is_empty()
            || self.id == "."
            || self.id == ".."
            || self.id.contains(['/', '\\'])
            || self.id.chars().any(char::is_control)
        {
            return Err(Error::Invalid(
                "aggregate identity is not a safe path segment".into(),
            ));
        }
        let segment = match self.kind {
            AggregateKind::Agent => "agents",
            AggregateKind::Conversation => "conversations",
            AggregateKind::Session => "sessions",
            AggregateKind::Turn => "turns",
            AggregateKind::Task => "tasks",
        };
        Ok(format!("harness/v2/{segment}/{}", self.id))
    }
}

/// Immutable authorization scope captured at admission.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Stable scope identity.
    id: String,
    /// Effective capabilities after policy intersection.
    capabilities: Capabilities,
    /// Host-attested agent on whose behalf this operation runs, if any.
    agent: Option<AgentId>,
    /// Trusted issuer identity.
    issuer: String,
    /// Parent proof, present for attenuated scopes.
    parent_proof: Option<[u8; 32]>,
    /// Keyed proof over the complete grant.
    proof: [u8; 32],
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Scope")
            .field("id", &self.id)
            .field("capabilities", &self.capabilities)
            .field("agent", &self.agent)
            .field("issuer", &self.issuer)
            .field("attenuated", &self.parent_proof.is_some())
            .field("proof", &"[redacted]")
            .finish()
    }
}

impl Scope {
    /// Returns the stable scope identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the immutable effective capability set.
    #[must_use]
    pub const fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Agent identity inherited unchanged by every attenuated scope.
    #[must_use]
    pub const fn agent(&self) -> Option<AgentId> {
        self.agent
    }

    #[cfg(any(feature = "host", feature = "wasm"))]
    pub(crate) fn from_wire(
        id: String,
        capabilities: Capabilities,
        issuer: String,
        agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
        proof: [u8; 32],
    ) -> Self {
        Self {
            id,
            capabilities,
            issuer,
            agent,
            parent_proof,
            proof,
        }
    }
}

/// Explicit host authority capable of issuing unforgeable scopes.
#[derive(Clone)]
pub struct AuthorityIssuer {
    id: String,
    key: [u8; 32],
    audience: Authority,
}

impl AuthorityIssuer {
    /// Creates an issuer from host-managed key material.
    #[must_use]
    pub fn new(id: impl Into<String>, key: [u8; 32], audience: Authority) -> Self {
        Self {
            id: id.into(),
            key,
            audience,
        }
    }

    /// Returns a verifier safe to give to reducers but not untrusted clients.
    #[must_use]
    pub fn verifier(&self) -> AuthorityVerifier {
        AuthorityVerifier {
            id: self.id.clone(),
            key: self.key,
            audience: self.audience.clone(),
        }
    }

    /// Issues one explicit root grant.
    #[must_use]
    pub fn root(&self, id: impl Into<String>, capabilities: Capabilities) -> Scope {
        self.issue(id.into(), capabilities, None, None)
    }

    /// Issues a root grant for a host-authenticated agent. The agent identity
    /// is part of the signed grant and cannot change during attenuation.
    #[must_use]
    pub fn root_for_agent(
        &self,
        agent: AgentId,
        id: impl Into<String>,
        capabilities: Capabilities,
    ) -> Scope {
        self.issue(id.into(), capabilities, Some(agent), None)
    }

    /// Issues a root grant after resolving all explicit hierarchy policy layers.
    pub fn root_with_policies(
        &self,
        id: impl Into<String>,
        layers: &[PolicyLayer],
    ) -> Result<Scope> {
        Ok(self.issue(id.into(), resolve_policy_layers(layers)?, None, None))
    }

    /// Resolves policy and signs the same acting-agent identity.
    pub fn root_with_policies_for_agent(
        &self,
        agent: AgentId,
        id: impl Into<String>,
        layers: &[PolicyLayer],
    ) -> Result<Scope> {
        Ok(self.issue(id.into(), resolve_policy_layers(layers)?, Some(agent), None))
    }

    /// Issues a child grant that cannot widen its parent.
    pub fn attenuate(
        &self,
        parent: &Scope,
        id: impl Into<String>,
        capabilities: Capabilities,
    ) -> Result<Scope> {
        self.verifier().verify(parent)?;
        if !capabilities.is_subset_of(&parent.capabilities) {
            return Err(Error::Unauthorized(
                "child scope cannot widen ancestor capabilities".into(),
            ));
        }
        Ok(self.issue(id.into(), capabilities, parent.agent, Some(parent.proof)))
    }

    /// Delegates one pinned private file to another agent without a fork or
    /// directory-wide grant. The original owner must already hold volume read
    /// authority; the recipient receives only an exact-version read scope.
    pub fn delegate_private_file_read(
        &self,
        owner_scope: &Scope,
        reader: AgentId,
        id: impl Into<String>,
        file: &FileRef,
    ) -> Result<Scope> {
        file.validate()?;
        let volume = file.volume();
        self.require_private_owner_read(owner_scope, volume)?;
        Ok(self.issue(
            id.into(),
            Capabilities::new([file.read_capability()?]),
            Some(reader),
            Some(owner_scope.proof),
        ))
    }

    /// Shares a private directory lazily with an agent outside any fork tree.
    /// The prefix is segment-bounded; this grant cannot authorize writes.
    pub fn delegate_private_directory_read(
        &self,
        owner_scope: &Scope,
        reader: AgentId,
        id: impl Into<String>,
        volume: &crate::conversation::VolumeRef,
        prefix: &str,
    ) -> Result<Scope> {
        self.require_private_owner_read(owner_scope, volume)?;
        Ok(self.issue(
            id.into(),
            Capabilities::new([volume.directory_read_capability(prefix)?]),
            Some(reader),
            Some(owner_scope.proof),
        ))
    }

    fn require_private_owner_read(
        &self,
        owner_scope: &Scope,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<()> {
        self.verifier().verify(owner_scope)?;
        volume.validate()?;
        if volume.class() != VolumeClass::AgentPrivate
            || !matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == owner_scope.agent)
            || !owner_scope
                .capabilities
                .contains(&volume.capability(VolumeOperation::Read)?)
        {
            return Err(Error::Unauthorized(
                "private read delegation requires its original owner".into(),
            ));
        }
        Ok(())
    }

    fn issue(
        &self,
        id: String,
        capabilities: Capabilities,
        agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
    ) -> Scope {
        let proof = scope_proof(
            &self.key,
            &self.audience,
            &self.id,
            &id,
            &capabilities,
            agent,
            parent_proof,
        );
        Scope {
            id,
            capabilities,
            issuer: self.id.clone(),
            agent,
            parent_proof,
            proof,
        }
    }

    /// Attests a provider observation after the host validates it against its registry.
    #[cfg(feature = "host")]
    pub(crate) fn attest_effect(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        attempt_id: EffectAttemptId,
        status: EffectStatus,
    ) -> Result<EffectAttestation> {
        let proof = effect_attestation_proof(
            &self.key,
            &self.audience,
            &effect.provider,
            effect_id,
            attempt_id,
            effect.request_digest,
            effect.guarantee,
            effect.result_schema_digest,
            &status,
        )?;
        Ok(EffectAttestation {
            provider: effect.provider.clone(),
            effect_id,
            attempt_id,
            request_digest: effect.request_digest,
            guarantee: effect.guarantee,
            result_schema_digest: effect.result_schema_digest,
            status,
            proof,
        })
    }
}

/// Reducer-side verifier for host-issued scopes.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthorityVerifier {
    id: String,
    key: [u8; 32],
    audience: Authority,
}

impl std::fmt::Debug for AuthorityVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorityVerifier")
            .field("id", &self.id)
            .field("audience", &self.audience)
            .field("key", &"[redacted]")
            .finish()
    }
}

impl AuthorityVerifier {
    /// Returns the one aggregate this verifier is allowed to authenticate.
    #[must_use]
    pub const fn audience(&self) -> &Authority {
        &self.audience
    }

    /// Verifies issuer identity and the complete immutable grant.
    pub fn verify(&self, scope: &Scope) -> Result<()> {
        let expected = scope_proof(
            &self.key,
            &self.audience,
            &scope.issuer,
            &scope.id,
            &scope.capabilities,
            scope.agent,
            scope.parent_proof,
        );
        if scope.issuer != self.id || scope.proof != expected {
            return Err(Error::Unauthorized("scope proof is invalid".into()));
        }
        Ok(())
    }

    fn attest_event(&self, event: &Event) -> Result<[u8; 32]> {
        let canonical = crate::contract::canonical_json_bytes(&(
            &self.audience,
            event.revision,
            event.operation_id,
            event.intent_digest,
            &event.scope,
            &event.causal_parent,
            &event.payload,
        ))?;
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(b"harness/v2/committed-event\0");
        hasher.update(&(canonical.len() as u64).to_le_bytes());
        hasher.update(&canonical);
        Ok(*hasher.finalize().as_bytes())
    }

    fn verify_event(&self, event: &Event) -> Result<()> {
        if event.scope.issuer != self.id || event.attestation != self.attest_event(event)? {
            return Err(Error::Unauthorized(
                "event admission attestation is invalid".into(),
            ));
        }
        Ok(())
    }

    /// Rejects accidental pairing with another aggregate authority.
    pub fn verify_audience(&self, authority: &Authority) -> Result<()> {
        if &self.audience == authority {
            Ok(())
        } else {
            Err(Error::Unauthorized(
                "authority verifier belongs to another aggregate".into(),
            ))
        }
    }

    fn verify_effect(&self, observation: &EffectAttestation) -> Result<()> {
        let expected = effect_attestation_proof(
            &self.key,
            &self.audience,
            &observation.provider,
            observation.effect_id,
            observation.attempt_id,
            observation.request_digest,
            observation.guarantee,
            observation.result_schema_digest,
            &observation.status,
        )?;
        if observation.proof != expected {
            return Err(Error::Unauthorized(
                "effect observation attestation is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Strength advertised by an external effect provider.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectGuarantee {
    /// Provider proves one externally observable application.
    ExactlyOnce,
    /// Provider accepts a stable retry identity and deduplicates retries.
    IdempotentRetry,
    /// Provider will not retry after an uncertain dispatch.
    AtMostOnce,
}

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
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Current durable status of an effect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "public ref-valued status keeps its stable serde and Rust shape"
)]
pub enum EffectStatus {
    /// Effect is durable but has not been dispatched.
    Planned,
    /// Dispatch began and its external outcome may still be unknown.
    Dispatched,
    /// Effect completed successfully.
    Succeeded {
        /// Pinned schema-defined JSON result file.
        result: FileRef,
    },
    /// Effect completed with a known failure.
    Failed {
        /// Stable failure description.
        message: String,
    },
    /// Provider cannot determine the externally visible outcome.
    Indeterminate,
}

impl EffectStatus {
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Indeterminate
        )
    }
}

/// Host-attested provider observation accepted by the pure reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectAttestation {
    /// Provider that produced the observation.
    pub provider: String,
    /// Effect being observed.
    pub effect_id: EffectId,
    /// Exact attempt being observed.
    pub attempt_id: EffectAttemptId,
    /// Digest of the immutable provider request.
    pub request_digest: [u8; 32],
    /// Pinned delivery guarantee.
    pub guarantee: EffectGuarantee,
    /// Pinned successful-result schema digest.
    pub result_schema_digest: [u8; 32],
    /// Terminal or explicitly unknown result.
    pub status: EffectStatus,
    proof: [u8; 32],
}

/// Complete durable projection of one external effect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectState {
    /// Provider selected before dispatch.
    pub provider: String,
    /// Provider-advertised delivery strength pinned for this effect.
    pub guarantee: EffectGuarantee,
    /// Namespaced effect kind.
    pub effect_kind: String,
    /// Immutable provider request file; the event carries no request body.
    pub request: FileRef,
    /// Canonical digest binding provider, kind, guarantee, and request.
    pub request_digest: [u8; 32],
    /// Immutable JSON Schema file for successful results.
    pub result_schema: FileRef,
    /// SHA-256 digest pinning the result schema bytes.
    pub result_schema_digest: [u8; 32],
    /// Current semantic completion state.
    pub status: EffectStatus,
    /// Ordered unique attempts made against the provider.
    pub attempts: Vec<EffectAttemptId>,
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

/// Non-bearer admission metadata retained in durable history. Its keyed
/// attestation belongs to the complete event, never to a reusable scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedScope {
    id: String,
    capabilities: Capabilities,
    issuer: String,
    agent: Option<AgentId>,
}

impl RecordedScope {
    fn from_scope(scope: &Scope) -> Self {
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

    #[cfg(any(feature = "host", feature = "wasm"))]
    pub(crate) fn wire_parts(&self) -> (&str, Option<AgentId>) {
        (&self.issuer, self.agent)
    }

    #[cfg(feature = "host")]
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

/// Versioned acceleration record; canonical events remain authoritative.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Snapshot format version.
    pub format_version: u32,
    /// Aggregate represented by the snapshot.
    pub authority: Authority,
    /// Last included event revision.
    pub revision: u64,
    /// Canonical events included in this portable v2 snapshot.
    pub events: Vec<Event>,
    /// Digest over all preceding fields.
    pub state_digest: [u8; 32],
}

/// Explicit treatment of one extension's state at a child fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionForkPolicy {
    /// Retain the selected immutable state revision for the child.
    Inherit,
    /// Require a fresh child-owned state revision.
    Reset,
    /// Refuse to fork while this extension state is selected.
    Reject,
}

/// One versioned extension state record. Bytes live only at `content`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRecord {
    /// Namespaced schema identity.
    pub name: String,
    /// Positive schema version.
    pub version: u32,
    /// Digest pinning the exact registered JSON Schema.
    pub schema_digest: [u8; 32],
    /// Digest pinning the admitted implementation.
    pub implementation_digest: [u8; 32],
    /// Fork treatment pinned at admission.
    pub fork_policy: ExtensionForkPolicy,
    /// Immutable JSON payload reference.
    pub content: FileRef,
}

impl ExtensionRecord {
    /// Checks the envelope before consulting its pinned schema registry.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0
            || self.schema_digest == [0; 32]
            || self.implementation_digest == [0; 32]
            || self.content.descriptor().media_type() != "application/json"
        {
            return Err(Error::Invalid("extension record is invalid".into()));
        }
        self.content.validate()
    }
}

fn validate_extension_name(name: &str) -> Result<()> {
    if name.len() > 256
        || !name.contains('.')
        || name.starts_with("acyclic.")
        || name.split('.').any(str::is_empty)
        || name
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(Error::Invalid(
            "extension name must be a bounded namespace".into(),
        ));
    }
    Ok(())
}

/// Immutable schema and implementation binding for one extension version.
#[derive(Clone, Debug, PartialEq)]
struct ExtensionBinding {
    schema: Value,
    schema_digest: [u8; 32],
    implementation_digest: [u8; 32],
    fork_policy: ExtensionForkPolicy,
}

/// Immutable-version registry for namespaced extension implementations and state schemas.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchemaRegistry {
    schemas: BTreeMap<(String, u32), ExtensionBinding>,
}

impl SchemaRegistry {
    /// Creates an empty registry that rejects every extension event.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schemas: BTreeMap::new(),
        }
    }

    /// Registers one immutable namespaced JSON Schema version.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        version: u32,
        schema: Value,
        implementation_digest: [u8; 32],
        fork_policy: ExtensionForkPolicy,
    ) -> Result<()> {
        let name = name.into();
        validate_extension_name(&name)?;
        if version == 0 || implementation_digest == [0; 32] {
            return Err(Error::Invalid(
                "extension binding needs a namespaced name, positive version, and implementation digest".into(),
            ));
        }
        jsonschema::validator_for(&schema)
            .map_err(|error| Error::Invalid(format!("invalid JSON Schema: {error}")))?;
        let binding = ExtensionBinding {
            schema_digest: json_digest(&schema)?,
            schema,
            implementation_digest,
            fork_policy,
        };
        let key = (name, version);
        if let Some(existing) = self.schemas.get(&key) {
            return if existing == &binding {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "extension identity is already pinned to another implementation".into(),
                ))
            };
        }
        self.schemas.insert(key, binding);
        Ok(())
    }

    fn pinned_binding(
        &self,
        name: &str,
        version: u32,
        expected: Option<([u8; 32], [u8; 32], ExtensionForkPolicy)>,
    ) -> Result<&ExtensionBinding> {
        let binding = self
            .schemas
            .get(&(name.to_owned(), version))
            .ok_or_else(|| {
                Error::Unsupported(format!("extension implementation {name}@{version}"))
            })?;
        if expected.is_some_and(|(schema, implementation, policy)| {
            schema != binding.schema_digest
                || implementation != binding.implementation_digest
                || policy != binding.fork_policy
        }) {
            return Err(Error::Conflict("extension binding mismatch".into()));
        }
        Ok(binding)
    }

    fn validate_bytes(
        &self,
        name: &str,
        version: u32,
        content: &FileRef,
        bytes: &[u8],
    ) -> Result<()> {
        content.validate()?;
        if content.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("extension content must be JSON".into()));
        }
        content.descriptor().verify(bytes)?;
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("extension content is not JSON: {error}")))?;
        let binding = self.pinned_binding(name, version, None)?;
        jsonschema::validator_for(&binding.schema)
            .map_err(|error| Error::Invalid(format!("invalid registered JSON Schema: {error}")))?
            .validate(&value)
            .map_err(|error| {
                Error::Invalid(format!("extension payload failed validation: {error}"))
            })?;
        Ok(())
    }
}

/// Restorable deterministic reducer state for one authority.
#[derive(Clone, Debug, PartialEq)]
pub struct Reducer {
    authority: Authority,
    authority_verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    revision: u64,
    lifecycle: LifecycleState,
    events: Vec<Event>,
    operation_intents: BTreeMap<OperationId, ([u8; 32], Event)>,
    effects: BTreeMap<EffectId, EffectState>,
    forks: BTreeMap<Authority, ForkSeed>,
    published_merges: BTreeSet<(String, [u8; 16])>,
    conversation: ConversationState,
    context_selections: Vec<ModelContextSelection>,
    interactions: BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
}

impl Reducer {
    /// Creates an empty aggregate reducer.
    #[must_use]
    pub fn new(
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Self {
        Self {
            authority,
            authority_verifier,
            schemas,
            revision: 0,
            lifecycle: LifecycleState::Pending,
            events: Vec::new(),
            operation_intents: BTreeMap::new(),
            effects: BTreeMap::new(),
            forks: BTreeMap::new(),
            published_merges: BTreeSet::new(),
            conversation: ConversationState::default(),
            context_selections: Vec::new(),
            interactions: BTreeMap::new(),
        }
    }

    /// Returns this reducer's authority.
    #[must_use]
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// Returns the current gapless revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Validates exact staged extension bytes at the provider admission boundary.
    pub fn validate_custom_bytes(
        &self,
        schema: &str,
        version: u32,
        content: &FileRef,
        bytes: &[u8],
    ) -> Result<()> {
        self.schemas.validate_bytes(schema, version, content, bytes)
    }

    /// Returns the fixed aggregate lifecycle projection.
    #[must_use]
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Returns the built-in conversation projection for conversation aggregates.
    #[must_use]
    pub fn conversation(&self) -> Option<&ConversationState> {
        (self.authority.kind == AggregateKind::Conversation).then_some(&self.conversation)
    }

    /// Returns revision-pinned model context selections in admission order.
    #[must_use]
    pub fn context_selections(&self) -> &[ModelContextSelection] {
        &self.context_selections
    }

    /// Returns the exact selection committed for one operation, even after
    /// later conversation messages changed the aggregate's current tail.
    #[must_use]
    pub fn context_selection_for_operation(
        &self,
        operation_id: OperationId,
    ) -> Option<&ModelContextSelection> {
        self.operation_intents
            .get(&operation_id)
            .and_then(|(_, event)| match &event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            })
    }

    /// Returns the exact admitted request and its optional terminal resolution.
    #[must_use]
    pub fn interaction(
        &self,
        id: &uuid::Uuid,
    ) -> Option<&(InteractionTicket, Option<InteractionResolution>)> {
        self.interactions.get(id)
    }

    /// Applies one deterministic command.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "public API of an already-published crate; taking a reference here would break \
                  existing external callers"
    )]
    pub fn apply(&mut self, command: Command) -> Result<ApplyResult> {
        match self.plan(&command)? {
            ApplyResult::Replayed { event } => Ok(ApplyResult::Replayed { event }),
            ApplyResult::Applied { event } => {
                self.apply_committed(event.clone())?;
                Ok(ApplyResult::Applied { event })
            }
        }
    }

    /// Plans a command without advancing authoritative state.
    ///
    /// Hosts append the returned event to Stream and call [`Self::apply_committed`]
    /// only after the append is known to have committed.
    #[allow(
        clippy::too_many_lines,
        reason = "one deterministic admission transition"
    )]
    pub fn plan(&self, command: &Command) -> Result<ApplyResult> {
        self.authority_verifier.verify_audience(&self.authority)?;
        IdempotencyKey::new(command.idempotency_key.0.clone())?;
        let intent = canonical_intent(command)?;
        if let Some((existing_intent, event)) = self.operation_intents.get(&command.operation_id) {
            if existing_intent == &intent {
                return Ok(ApplyResult::Replayed {
                    event: event.clone(),
                });
            }
            return Err(Error::Conflict(
                "operation identity is already bound to another intent".into(),
            ));
        }
        if command.expected_revision != self.revision {
            return Err(Error::Conflict(format!(
                "expected revision {}, found {}",
                command.expected_revision, self.revision
            )));
        }
        self.authority_verifier.verify(&command.scope)?;
        require_capability(&command.scope, command.action.required_capability())?;
        if let Action::BindConversation { agent } = &command.action
            && command.scope.agent().is_some_and(|acting| acting != *agent)
        {
            return Err(Error::Unauthorized(
                "conversation binding does not match the authenticated agent".into(),
            ));
        }
        match &command.action {
            Action::PlanEffect { .. } => require_capability(&command.scope, "effect:plan")?,
            Action::MarkEffectDispatched { effect_id, .. } => {
                let effect = self
                    .effects
                    .get(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                require_capability(
                    &command.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            Action::ResolveEffect { observation } => {
                let effect = self
                    .effects
                    .get(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                require_capability(
                    &command.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            Action::ResolveInteraction { resolution } => {
                let (ticket, _) = self
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                require_capability(&command.scope, &ticket.responder_grant())?;
            }
            _ => {}
        }
        if let Action::PublishFork { seed } = &command.action {
            if seed.validate().is_err()
                || seed.operation_id != command.operation_id
                || seed.parent != self.authority
                || seed.parent_revision != self.revision
                || self.validate_fork_reference_ownership(seed).is_err()
            {
                return Err(Error::Invalid(
                    "fork manifest is not bound to its command and parent revision".into(),
                ));
            }
            self.require_fresh_fork(seed)?;
        }
        if let Action::PublishProjectMerge { receipt } = &command.action
            && (receipt.operation_id != command.operation_id
                || command.scope.agent() != self.conversation.agent)
        {
            return Err(Error::Unauthorized(
                "merge receipt is not bound to the parent agent and operation".into(),
            ));
        }
        validate_causal_parent(
            &self.authority,
            self.revision,
            command.causal_parent.as_ref(),
        )?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("aggregate revision exhausted".into()))?;
        let payload = self.transition(&command.action)?;
        let mut event = Event {
            revision,
            operation_id: command.operation_id,
            intent_digest: intent,
            scope: RecordedScope::from_scope(&command.scope),
            attestation: [0; 32],
            causal_parent: command.causal_parent.clone(),
            payload,
        };
        event.attestation = self.authority_verifier.attest_event(&event)?;
        Ok(ApplyResult::Applied { event })
    }

    /// Applies an event only after its canonical Stream append commits.
    pub fn apply_committed(&mut self, event: Event) -> Result<ApplyResult> {
        self.authority_verifier.verify_audience(&self.authority)?;
        if let Some((digest, existing)) = self.operation_intents.get(&event.operation_id) {
            if digest == &event.intent_digest && existing == &event {
                return Ok(ApplyResult::Replayed {
                    event: existing.clone(),
                });
            }
            return Err(Error::Conflict(
                "operation identity is already bound to another event".into(),
            ));
        }
        let expected = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("aggregate revision exhausted".into()))?;
        if event.revision != expected {
            return Err(Error::Conflict(format!(
                "expected committed revision {expected}, found {}",
                event.revision
            )));
        }
        self.authority_verifier.verify_event(&event)?;
        require_recorded_capability(&event.scope, event.payload.required_capability())?;
        match &event.payload {
            EventPayload::EffectPlanned { .. } => {
                require_recorded_capability(&event.scope, "effect:plan")?;
            }
            EventPayload::EffectDispatched { effect_id, .. } => {
                let effect = self
                    .effects
                    .get(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                require_recorded_capability(
                    &event.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .effects
                    .get(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                require_recorded_capability(
                    &event.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            EventPayload::InteractionResolved { resolution } => {
                let (ticket, _) = self
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                require_recorded_capability(&event.scope, &ticket.responder_grant())?;
            }
            _ => {}
        }
        validate_causal_parent(&self.authority, self.revision, event.causal_parent.as_ref())?;
        if let EventPayload::ForkPublished { seed } = &event.payload
            && (seed.validate().is_err()
                || seed.operation_id != event.operation_id
                || seed.parent != self.authority
                || seed.parent_revision != self.revision
                || self.validate_fork_reference_ownership(seed).is_err())
        {
            return Err(Error::Invalid("fork event binding is invalid".into()));
        }
        if let EventPayload::ProjectMergePublished { receipt } = &event.payload
            && (receipt.operation_id != event.operation_id
                || event.scope.agent() != self.conversation.agent)
        {
            return Err(Error::Unauthorized(
                "project merge was not published by the parent agent".into(),
            ));
        }
        self.apply_payload(&event.payload)?;
        self.revision = event.revision;
        self.events.push(event.clone());
        self.operation_intents
            .insert(event.operation_id, (event.intent_digest, event.clone()));
        Ok(ApplyResult::Applied { event })
    }

    /// Returns a bounded page strictly after `revision`.
    pub fn events_after(&self, revision: u64, limit: usize) -> Result<Vec<Event>> {
        if revision > self.revision {
            return Err(Error::Invalid("cursor is beyond the aggregate head".into()));
        }
        Ok(self
            .events
            .iter()
            .skip_while(|event| event.revision <= revision)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Returns the current effect status.
    #[must_use]
    pub fn effect(&self, effect_id: EffectId) -> Option<&EffectState> {
        self.effects.get(&effect_id)
    }

    /// Returns a child only after its publication event committed.
    #[must_use]
    pub fn fork(&self, child: &Authority) -> Option<&ForkSeed> {
        self.forks.get(child)
    }

    fn require_fresh_fork(&self, seed: &ForkSeed) -> Result<()> {
        if seed.child == self.authority
            || self.forks.contains_key(&seed.child)
            || self.forks.values().any(|prior| {
                prior.child_agent == seed.child_agent
                    || prior.child_private_volume == seed.child_private_volume
            })
        {
            Err(Error::Conflict("invalid fork publication".into()))
        } else {
            Ok(())
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "fork publication checks one complete seed"
    )]
    fn validate_fork_reference_ownership(&self, seed: &ForkSeed) -> Result<()> {
        let inherited_count = usize::try_from(seed.inherited_through_sequence)
            .map_err(|_| Error::Invalid("inherited conversation prefix is too large".into()))?;
        let parent_agent = self
            .conversation
            .agent
            .ok_or_else(|| Error::Conflict("fork parent conversation is unbound".into()))?;
        if seed.child_agent == parent_agent {
            return Err(Error::Invalid(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        if seed.inherited_through_sequence > self.conversation.messages.len() as u64 {
            return Err(Error::Invalid(
                "fork inherited prefix exceeds parent conversation".into(),
            ));
        }
        let materialized = seed
            .inherited_context
            .iter()
            .find(|file| file.path() == ".system/inherited-conversation/prefix.json");
        if seed.inherited_through_sequence > 0 && materialized.is_none() {
            return Err(Error::Invalid(
                "fork omitted its inherited conversation prefix".into(),
            ));
        }
        if let Some(file) = materialized {
            let prefix = InheritedConversationPrefix::select(
                self.authority.clone(),
                self.revision,
                parent_agent,
                seed.inherited_through_sequence,
                &seed.attached_agents,
                &self.conversation.messages,
            )?;
            let bytes = prefix.canonical_bytes()?;
            let expected = FileDescriptor::from_bytes(
                &bytes,
                "application/vnd.acyclic.harness.inherited-conversation+json",
            )?;
            if file.descriptor() != &expected
                || file.display_name() != "inherited-conversation.json"
            {
                return Err(Error::Invalid(
                    "inherited conversation differs from authoritative parent history".into(),
                ));
            }
        }
        let mut published_refs = std::collections::BTreeMap::new();
        let mut published_manifests = std::collections::BTreeSet::new();
        for message in self.conversation.messages.iter().take(inherited_count) {
            let mut retain = |file: &crate::conversation::FileRef| -> Result<()> {
                published_refs.insert(file.read_capability()?, file.clone());
                Ok(())
            };
            retain(&message.content)?;
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items {
                        retain(&item.file)?;
                    }
                }
                crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
                    published_manifests.insert(manifest.read_capability()?);
                    retain(manifest)?;
                }
            }
            for file in message.extensions.values() {
                retain(file)?;
            }
        }
        let selected_manifests = seed
            .attachment_manifests
            .iter()
            .map(crate::conversation::FileRef::read_capability)
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        if selected_manifests != published_manifests {
            return Err(Error::Invalid(
                "fork attachment manifests do not match the parent transcript".into(),
            ));
        }
        let granted = seed
            .reference_grants
            .iter()
            .map(|grant| Ok((grant.capability()?, grant.reader)))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        for reader in std::iter::once(seed.child_agent).chain(seed.attached_agents.iter().copied())
        {
            for (capability, file) in &published_refs {
                if file.volume().class() == crate::conversation::VolumeClass::AgentPrivate
                    && file.volume().owner() == &crate::conversation::VolumeOwner::Agent(reader)
                {
                    continue;
                }
                if !granted.contains(&(capability.clone(), reader)) {
                    return Err(Error::Invalid(
                        "fork omits a published reference grant".into(),
                    ));
                }
            }
        }
        for grant in &seed.reference_grants {
            let file = &grant.file;
            if file.volume() == &seed.child_private_volume {
                if grant.attachment_manifest.is_some() || !seed.inherited_context.contains(file) {
                    return Err(Error::Invalid(
                        "fork reference is not inherited context".into(),
                    ));
                }
            } else if file.volume().owner()
                == &crate::conversation::VolumeOwner::Agent(parent_agent)
                || seed.resources.iter().any(|capture| match &capture.source {
                    crate::fork::ResourceRevision::Project { volume, .. }
                    | crate::fork::ResourceRevision::SharedVolume(volume) => {
                        volume == file.volume()
                    }
                    _ => false,
                })
            {
                let published =
                    self.conversation
                        .messages
                        .iter()
                        .take(inherited_count)
                        .any(|message| {
                            message.content == *file
                                || match &message.attachments {
                                    crate::conversation::ReferencedAttachments::Inline {
                                        items,
                                    } => items.iter().any(|item| item.file == *file),
                                    crate::conversation::ReferencedAttachments::Manifest {
                                        manifest,
                                        ..
                                    } => manifest == file,
                                }
                                || message.extensions.values().any(|value| value == file)
                        });
                match &grant.attachment_manifest {
                    None if !published => {
                        return Err(Error::Invalid(
                            "fork reference was not published by parent".into(),
                        ));
                    }
                    Some(manifest)
                        if !published_manifests.contains(&manifest.read_capability()?) =>
                    {
                        return Err(Error::Invalid(
                            "fork reference manifest is not a published attachment list".into(),
                        ));
                    }
                    _ => {}
                }
            } else {
                return Err(Error::Unauthorized(
                    "fork cannot delegate another owner's file".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_extension_fork(&self, seed: &ForkSeed) -> Result<()> {
        for capture in &seed.resources {
            if let crate::fork::ResourceRevision::Extension {
                name,
                version,
                implementation_digest,
                ..
            } = &capture.revision
            {
                let binding = self.schemas.pinned_binding(name, *version, None)?;
                if binding.implementation_digest != *implementation_digest {
                    return Err(Error::Conflict(
                        "fork extension implementation mismatch".into(),
                    ));
                }
                match binding.fork_policy {
                    ExtensionForkPolicy::Inherit if capture.source == capture.revision => {}
                    ExtensionForkPolicy::Reset if capture.source != capture.revision => {}
                    ExtensionForkPolicy::Reject => {
                        return Err(Error::Unsupported(format!(
                            "extension {name}@{version} cannot be forked"
                        )));
                    }
                    _ => {
                        return Err(Error::Conflict(format!(
                            "extension {name}@{version} capture violates its fork policy"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Creates a portable, integrity-checked restoration accelerator.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot {
            format_version: 2,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self.events.clone(),
            state_digest: [0; 32],
        };
        snapshot.state_digest = snapshot_digest(&snapshot)?;
        Ok(snapshot)
    }

    /// Restores deterministic state after validating snapshot integrity.
    pub fn restore(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Result<Self> {
        if snapshot.format_version != 2 {
            return Err(Error::Unsupported(format!(
                "snapshot format {}",
                snapshot.format_version
            )));
        }
        if snapshot_digest(&snapshot)? != snapshot.state_digest {
            return Err(Error::Invalid("snapshot digest mismatch".into()));
        }
        authority_verifier.verify_audience(&snapshot.authority)?;
        let mut reducer = Self::new(snapshot.authority, authority_verifier, schemas);
        for event in snapshot.events {
            reducer.apply_committed(event)?;
        }
        if reducer.revision != snapshot.revision {
            return Err(Error::Invalid(
                "snapshot revision does not match its events".into(),
            ));
        }
        Ok(reducer)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one match arm per Action variant, each independently validating and building \
                  its EventPayload; splitting per-arm would scatter one command's validation \
                  across many functions without clarifying any of them"
    )]
    fn transition(&self, action: &Action) -> Result<EventPayload> {
        match action {
            Action::TransitionLifecycle { to, reason } => {
                validate_lifecycle(self.lifecycle, *to)?;
                Ok(EventPayload::LifecycleTransitioned {
                    from: self.lifecycle,
                    to: *to,
                    reason: reason.clone(),
                })
            }
            Action::AppendCustom {
                schema,
                version,
                content,
            } => {
                content.validate()?;
                if content.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("extension content must be JSON".into()));
                }
                let binding = self.schemas.pinned_binding(schema, *version, None)?;
                let record = ExtensionRecord {
                    name: schema.clone(),
                    version: *version,
                    schema_digest: binding.schema_digest,
                    implementation_digest: binding.implementation_digest,
                    fork_policy: binding.fork_policy,
                    content: content.clone(),
                };
                record.validate()?;
                Ok(EventPayload::Custom { record })
            }
            Action::PlanEffect {
                effect_id,
                provider,
                guarantee,
                effect_kind,
                request,
                result_schema,
            } => {
                if self.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if provider.trim().is_empty() || effect_kind.trim().is_empty() {
                    return Err(Error::Invalid(
                        "effect provider and kind must be non-empty".into(),
                    ));
                }
                request.validate()?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                let request_digest =
                    effect_request_digest(provider, *guarantee, effect_kind, request)?;
                result_schema.validate()?;
                if result_schema.descriptor().media_type() != "application/schema+json" {
                    return Err(Error::Invalid(
                        "effect result schema must be JSON Schema content".into(),
                    ));
                }
                let result_schema_digest = *result_schema.descriptor().sha256();
                Ok(EventPayload::EffectPlanned {
                    effect_id: *effect_id,
                    provider: provider.clone(),
                    guarantee: *guarantee,
                    effect_kind: effect_kind.clone(),
                    request: request.clone(),
                    request_digest,
                    result_schema: result_schema.clone(),
                    result_schema_digest,
                })
            }
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            } => {
                let effect = self
                    .effects
                    .get(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                let may_retry = effect.status == EffectStatus::Indeterminate
                    && effect.guarantee == EffectGuarantee::IdempotentRetry;
                if effect.status != EffectStatus::Planned && !may_retry {
                    return Err(Error::Conflict(
                        "effect cannot start another dispatch attempt".into(),
                    ));
                }
                if effect.attempts.contains(attempt_id) {
                    return Err(Error::Conflict(
                        "effect attempt identity already exists".into(),
                    ));
                }
                Ok(EventPayload::EffectDispatched {
                    effect_id: *effect_id,
                    attempt_id: *attempt_id,
                })
            }
            Action::ResolveEffect { observation } => {
                if !observation.status.is_terminal() {
                    return Err(Error::Invalid(
                        "effect resolution must be terminal or indeterminate".into(),
                    ));
                }
                let effect = self
                    .effects
                    .get(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                self.authority_verifier.verify_effect(observation)?;
                validate_effect_observation(effect, observation)?;
                if effect.attempts.last() != Some(&observation.attempt_id) {
                    return Err(Error::Conflict(
                        "effect result does not match its active attempt".into(),
                    ));
                }
                if !matches!(
                    effect.status,
                    EffectStatus::Dispatched | EffectStatus::Indeterminate
                ) {
                    return Err(Error::Conflict("effect already has another result".into()));
                }
                validate_effect_result(&observation.status)?;
                Ok(EventPayload::EffectResolved {
                    observation: observation.clone(),
                })
            }
            Action::PublishFork { seed } => {
                seed.validate()?;
                self.validate_fork_reference_ownership(seed)?;
                self.validate_extension_fork(seed)?;
                if self.forks.contains_key(&seed.child) {
                    return Err(Error::Conflict("fork child is already published".into()));
                }
                Ok(EventPayload::ForkPublished { seed: seed.clone() })
            }
            Action::PublishProjectMerge { receipt } => {
                self.require_conversation()?;
                self.require_bound_conversation_if_applicable()?;
                let seed = self
                    .forks
                    .get(&receipt.child)
                    .ok_or_else(|| Error::NotFound("merge child was not published".into()))?;
                receipt.validate(seed)?;
                let key = (
                    receipt.target_project.storage_name()?,
                    receipt.filesystem_operation_id,
                );
                if self.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                let mut next = self.conversation.clone();
                next.append(receipt.notice.clone())?;
                Ok(EventPayload::ProjectMergePublished {
                    receipt: receipt.clone(),
                })
            }
            Action::BindConversation { agent } => {
                self.require_conversation()?;
                let mut next = self.conversation.clone();
                next.bind(*agent)?;
                Ok(EventPayload::ConversationBound { agent: *agent })
            }
            Action::AppendConversationMessage { message } => {
                self.require_conversation()?;
                let mut next = self.conversation.clone();
                next.append((**message).clone())?;
                Ok(EventPayload::ConversationMessageAppended {
                    message: message.clone(),
                })
            }
            Action::SelectModelContext { selection } => {
                self.require_conversation()?;
                selection.validate(&self.conversation)?;
                Ok(EventPayload::ModelContextSelected {
                    selection: selection.clone(),
                })
            }
            Action::OpenInteraction { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate()?;
                if self.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                Ok(EventPayload::InteractionOpened {
                    ticket: ticket.clone(),
                })
            }
            Action::ResolveInteraction { resolution } => {
                let (ticket, prior) = self
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                Ok(EventPayload::InteractionResolved {
                    resolution: resolution.clone(),
                })
            }
        }
    }

    fn require_conversation(&self) -> Result<()> {
        if self.authority.kind != AggregateKind::Conversation {
            return Err(Error::Invalid(
                "conversation command targets another aggregate".into(),
            ));
        }
        Ok(())
    }

    fn require_bound_conversation_if_applicable(&self) -> Result<()> {
        if self.authority.kind == AggregateKind::Conversation && self.conversation.agent.is_none() {
            return Err(Error::Conflict(
                "conversation is not bound to an agent".into(),
            ));
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one match arm per EventPayload variant, mirroring transition() above; each arm \
                  independently re-validates and commits its own state, so splitting per-arm \
                  would scatter one event's application across many functions without \
                  clarifying any of them"
    )]
    fn apply_payload(&mut self, payload: &EventPayload) -> Result<()> {
        match payload {
            EventPayload::LifecycleTransitioned { from, to, .. } => {
                if *from != self.lifecycle {
                    return Err(Error::Conflict("lifecycle predecessor mismatch".into()));
                }
                validate_lifecycle(*from, *to)?;
                self.lifecycle = *to;
            }
            EventPayload::Custom { record } => {
                record.validate()?;
                self.schemas.pinned_binding(
                    &record.name,
                    record.version,
                    Some((
                        record.schema_digest,
                        record.implementation_digest,
                        record.fork_policy,
                    )),
                )?;
            }
            EventPayload::EffectPlanned {
                effect_id,
                provider,
                guarantee,
                effect_kind,
                request,
                request_digest,
                result_schema,
                result_schema_digest,
            } => {
                if provider.trim().is_empty() || effect_kind.trim().is_empty() {
                    return Err(Error::Invalid(
                        "effect provider and kind must be non-empty".into(),
                    ));
                }
                request.validate()?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                if self.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if *request_digest
                    != effect_request_digest(provider, *guarantee, effect_kind, request)?
                {
                    return Err(Error::Invalid("effect request digest mismatch".into()));
                }
                result_schema.validate()?;
                if result_schema.descriptor().media_type() != "application/schema+json"
                    || *result_schema_digest != *result_schema.descriptor().sha256()
                {
                    return Err(Error::Invalid(
                        "effect result schema digest mismatch".into(),
                    ));
                }
                self.effects.insert(
                    *effect_id,
                    EffectState {
                        provider: provider.clone(),
                        guarantee: *guarantee,
                        effect_kind: effect_kind.clone(),
                        request: request.clone(),
                        request_digest: *request_digest,
                        result_schema: result_schema.clone(),
                        result_schema_digest: *result_schema_digest,
                        status: EffectStatus::Planned,
                        attempts: Vec::new(),
                    },
                );
            }
            EventPayload::EffectDispatched {
                effect_id,
                attempt_id,
            } => {
                let effect = self
                    .effects
                    .get_mut(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                let may_retry = effect.status == EffectStatus::Indeterminate
                    && effect.guarantee == EffectGuarantee::IdempotentRetry;
                if effect.status != EffectStatus::Planned && !may_retry {
                    return Err(Error::Conflict("invalid effect dispatch transition".into()));
                }
                if effect.attempts.contains(attempt_id) {
                    return Err(Error::Conflict(
                        "effect attempt identity already exists".into(),
                    ));
                }
                effect.attempts.push(*attempt_id);
                effect.status = EffectStatus::Dispatched;
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .effects
                    .get_mut(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                self.authority_verifier.verify_effect(observation)?;
                validate_effect_observation(effect, observation)?;
                if !observation.status.is_terminal()
                    || effect.attempts.last() != Some(&observation.attempt_id)
                    || !matches!(
                        effect.status,
                        EffectStatus::Dispatched | EffectStatus::Indeterminate
                    )
                {
                    return Err(Error::Conflict(
                        "invalid effect resolution transition".into(),
                    ));
                }
                validate_effect_result(&observation.status)?;
                effect.status = observation.status.clone();
            }
            EventPayload::ForkPublished { seed } => {
                seed.validate()?;
                self.validate_extension_fork(seed)?;
                self.require_fresh_fork(seed)?;
                self.forks.insert(seed.child.clone(), seed.as_ref().clone());
            }
            EventPayload::ProjectMergePublished { receipt } => {
                self.require_conversation()?;
                let seed = self
                    .forks
                    .get(&receipt.child)
                    .ok_or_else(|| Error::NotFound("merge child was not published".into()))?;
                receipt.validate(seed)?;
                let key = (
                    receipt.target_project.storage_name()?,
                    receipt.filesystem_operation_id,
                );
                if self.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                self.conversation.append(receipt.notice.clone())?;
                self.published_merges.insert(key);
            }
            EventPayload::ConversationBound { agent } => {
                self.require_conversation()?;
                self.conversation.bind(*agent)?;
            }
            EventPayload::ConversationMessageAppended { message } => {
                self.require_conversation()?;
                self.conversation.append((**message).clone())?;
            }
            EventPayload::ModelContextSelected { selection } => {
                self.require_conversation()?;
                selection.validate(&self.conversation)?;
                self.context_selections.push(selection.clone());
            }
            EventPayload::InteractionOpened { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate()?;
                if self.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                self.interactions.insert(ticket.id, (ticket.clone(), None));
            }
            EventPayload::InteractionResolved { resolution } => {
                let (ticket, prior) = self
                    .interactions
                    .get_mut(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                *prior = Some(resolution.clone());
            }
        }
        Ok(())
    }
}

impl Action {
    fn required_capability(&self) -> &'static str {
        match self {
            Self::TransitionLifecycle { .. } => "lifecycle:manage",
            Self::AppendCustom { .. } => "event:append",
            Self::PlanEffect { .. }
            | Self::MarkEffectDispatched { .. }
            | Self::ResolveEffect { .. } => "effect:run",
            Self::PublishFork { .. } => "fork:publish",
            Self::PublishProjectMerge { .. } => "project:merge",
            Self::BindConversation { .. } => "conversation:bind",
            Self::AppendConversationMessage { .. } => "conversation:append",
            Self::SelectModelContext { .. } => "conversation:select_context",
            Self::OpenInteraction { .. } => "interaction:open",
            Self::ResolveInteraction { .. } => "interaction:resolve",
        }
    }
}

impl EventPayload {
    fn required_capability(&self) -> &'static str {
        match self {
            Self::LifecycleTransitioned { .. } => "lifecycle:manage",
            Self::Custom { .. } => "event:append",
            Self::EffectPlanned { .. }
            | Self::EffectDispatched { .. }
            | Self::EffectResolved { .. } => "effect:run",
            Self::ForkPublished { .. } => "fork:publish",
            Self::ProjectMergePublished { .. } => "project:merge",
            Self::ConversationBound { .. } => "conversation:bind",
            Self::ConversationMessageAppended { .. } => "conversation:append",
            Self::ModelContextSelected { .. } => "conversation:select_context",
            Self::InteractionOpened { .. } => "interaction:open",
            Self::InteractionResolved { .. } => "interaction:resolve",
        }
    }
}

fn validate_interaction_transition(
    ticket: &InteractionTicket,
    prior: Option<&InteractionResolution>,
    resolution: &InteractionResolution,
) -> Result<()> {
    resolution.validate(ticket)?;
    if prior.is_some_and(|value| value.outcome.is_terminal()) {
        return Err(Error::Conflict("interaction is already resolved".into()));
    }
    let expected = prior.map_or(Ok(1_u64), |value| {
        value
            .expected_version
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("interaction revision exhausted".into()))
    })?;
    if resolution.expected_version != expected {
        return Err(Error::Conflict(
            "interaction resolution version mismatch".into(),
        ));
    }
    Ok(())
}

fn require_capability(scope: &Scope, capability: &str) -> Result<()> {
    if scope.capabilities.contains(capability) {
        Ok(())
    } else {
        Err(Error::Unauthorized(format!(
            "scope {} lacks capability {capability}",
            scope.id
        )))
    }
}

fn require_recorded_capability(scope: &RecordedScope, capability: &str) -> Result<()> {
    if scope.capabilities.contains(capability) {
        Ok(())
    } else {
        Err(Error::Unauthorized(format!(
            "recorded scope {} lacks capability {capability}",
            scope.id
        )))
    }
}

fn validate_causal_parent(
    authority: &Authority,
    current_revision: u64,
    reference: Option<&EventReference>,
) -> Result<()> {
    let Some(reference) = reference else {
        return Ok(());
    };
    if reference.revision == 0 {
        return Err(Error::Invalid(
            "causal event revision must be positive".into(),
        ));
    }
    if reference.authority == *authority && reference.revision > current_revision {
        return Err(Error::Invalid("causal event is in the future".into()));
    }
    reference.authority.stream_path()?;
    Ok(())
}

fn validate_lifecycle(from: LifecycleState, to: LifecycleState) -> Result<()> {
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

fn scope_proof(
    key: &[u8; 32],
    audience: &Authority,
    issuer: &str,
    id: &str,
    capabilities: &Capabilities,
    agent: Option<AgentId>,
    parent_proof: Option<[u8; 32]>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(&[match audience.kind {
        AggregateKind::Agent => 1,
        AggregateKind::Conversation => 2,
        AggregateKind::Session => 3,
        AggregateKind::Turn => 4,
        AggregateKind::Task => 5,
    }]);
    hasher.update(&(audience.id.len() as u64).to_le_bytes());
    hasher.update(audience.id.as_bytes());
    for value in [issuer, id] {
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    for capability in capabilities.iter() {
        hasher.update(&(capability.len() as u64).to_le_bytes());
        hasher.update(capability.as_bytes());
    }
    match agent {
        Some(agent) => {
            hasher.update(&[1]);
            hasher.update(&agent.into_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    match parent_proof {
        Some(parent) => {
            hasher.update(&[1]);
            hasher.update(&parent);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    *hasher.finalize().as_bytes()
}

#[allow(clippy::too_many_arguments)]
fn effect_attestation_proof(
    key: &[u8; 32],
    audience: &Authority,
    provider: &str,
    effect_id: EffectId,
    attempt_id: EffectAttemptId,
    request_digest: [u8; 32],
    guarantee: EffectGuarantee,
    result_schema_digest: [u8; 32],
    status: &EffectStatus,
) -> Result<[u8; 32]> {
    let bytes = crate::contract::canonical_json_bytes(&(
        audience,
        provider,
        effect_id,
        attempt_id,
        request_digest,
        guarantee,
        result_schema_digest,
        status,
    ))?;
    Ok(*blake3::keyed_hash(key, &bytes).as_bytes())
}

pub(crate) fn canonical_intent(command: &Command) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(command)
}

fn effect_request_digest(
    provider: &str,
    guarantee: EffectGuarantee,
    effect_kind: &str,
    request: &FileRef,
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(provider, guarantee, effect_kind, request))
}

fn validate_effect_result(status: &EffectStatus) -> Result<()> {
    if let EffectStatus::Succeeded { result } = status {
        result.validate()?;
        if result.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("effect result must be JSON content".into()));
        }
    }
    Ok(())
}

fn validate_effect_observation(
    effect: &EffectState,
    observation: &EffectAttestation,
) -> Result<()> {
    if observation.provider != effect.provider
        || observation.request_digest != effect.request_digest
        || observation.guarantee != effect.guarantee
        || observation.result_schema_digest != effect.result_schema_digest
    {
        return Err(Error::Conflict(
            "effect observation does not match durable state".into(),
        ));
    }
    Ok(())
}

fn snapshot_digest(snapshot: &Snapshot) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(
        snapshot.format_version,
        &snapshot.authority,
        snapshot.revision,
        &snapshot.events,
    ))
}

fn json_digest(value: &Value) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn v2_extension_record_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/extension-record.json").trim();
        let record: ExtensionRecord =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        record.validate()?;
        let encoded =
            serde_json::to_string(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn extension_registry_and_records_share_a_bounded_name_policy() -> Result<()> {
        for invalid in [
            "plain",
            "example.",
            ".example",
            "example..state",
            "example. state",
            "example.\0state",
        ] {
            assert!(validate_extension_name(invalid).is_err());
            assert!(
                SchemaRegistry::new()
                    .register(
                        invalid,
                        1,
                        json!({"type":"object"}),
                        [1; 32],
                        ExtensionForkPolicy::Inherit
                    )
                    .is_err()
            );
        }
        assert!(validate_extension_name(&format!("example.{}", "x".repeat(256))).is_err());
        validate_extension_name("example.state")?;
        Ok(())
    }

    fn operation(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
    }
    fn request_file(bytes: &[u8]) -> Result<FileRef> {
        use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
        use crate::resources::ProviderRef;
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "effects",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
            )?,
            "requests/request.json",
            "generation-1",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "request.json",
        )
    }
    fn schema_file(schema: &Value) -> Result<FileRef> {
        use crate::conversation::FileDescriptor;
        let bytes =
            serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?;
        FileRef::new(
            request_file(b"null")?.volume().clone(),
            "schemas/result.json",
            "generation-1",
            FileDescriptor::from_bytes(&bytes, "application/schema+json")?,
            "result.json",
        )
    }
    fn issuer() -> AuthorityIssuer {
        AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
        )
    }
    fn schemas() -> SchemaRegistry {
        let mut schemas = SchemaRegistry::new();
        assert!(
            schemas
                .register(
                    "example.message",
                    1,
                    json!({
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "additionalProperties": true
                    }),
                    [9; 32],
                    ExtensionForkPolicy::Inherit,
                )
                .is_ok()
        );
        schemas
    }
    fn command(operation_id: OperationId, expected_revision: u64, action: Action) -> Command {
        Command {
            operation_id,
            idempotency_key: IdempotencyKey(format!("key-{operation_id}")),
            expected_revision,
            scope: issuer().root(
                "root",
                Capabilities::new([
                    "event:append",
                    "lifecycle:manage",
                    "effect:run",
                    "effect:plan",
                    "effect:provider:example.provider",
                    "interaction:open",
                    "interaction:respond",
                    "fork:publish",
                    "conversation:bind",
                    "conversation:append",
                    "conversation:select_context",
                ]),
            ),
            causal_parent: None,
            action,
        }
    }

    #[test]
    fn committed_events_attest_admission_without_persisting_bearer_scopes() -> Result<()> {
        let verifier_debug = format!("{:?}", issuer().verifier());
        assert!(verifier_debug.contains("[redacted]"));
        assert!(!verifier_debug.contains("[7, 7, 7"));
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let mut reducer = Reducer::new(authority, issuer().verifier(), schemas());
        let planned = reducer.plan(&command(
            operation(19),
            0,
            Action::BindConversation {
                agent: AgentId::from_bytes([19; 16]),
            },
        ))?;
        let ApplyResult::Applied { event } = planned else {
            return Err(Error::Storage("fresh event replayed".into()));
        };
        let json =
            serde_json::to_value(&event).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(json["scope"].get("proof").is_none());
        assert!(json["scope"].get("parent_proof").is_none());
        assert!(serde_json::from_value::<Scope>(json["scope"].clone()).is_err());
        let mut tampered = event.clone();
        tampered.scope.capabilities = Capabilities::new(["conversation:bind", "effect:run"]);
        assert!(matches!(
            reducer.apply_committed(tampered),
            Err(Error::Unauthorized(_))
        ));
        reducer.apply_committed(event)?;
        assert_eq!(reducer.revision(), 1);
        Ok(())
    }

    #[test]
    fn interactions_are_ref_only_versioned_and_responder_bound() -> Result<()> {
        use crate::interaction::{Interaction, InteractionKind, InteractionOutcome};
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        let request = Interaction::approval("approve exact effect", operation(12), [3; 32])?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes([5; 16]),
            kind: InteractionKind::Approval,
            request: request_file(&request_bytes)?,
            deadline_unix_ms: Some(1_000),
            approval: Some(crate::interaction::ApprovalBinding {
                operation_id: operation(12),
                action_digest: [3; 32],
            }),
        };
        ticket.validate_request_bytes(&request_bytes)?;
        reducer.apply(command(
            operation(19),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        let opened = command(
            operation(20),
            1,
            Action::OpenInteraction {
                ticket: ticket.clone(),
            },
        );
        assert!(matches!(
            reducer.apply(opened)?,
            ApplyResult::Applied { .. }
        ));
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version: 2,
            outcome: InteractionOutcome::Approved,
            detail: None,
        };
        let ungranted = command(
            operation(21),
            2,
            Action::ResolveInteraction {
                resolution: resolution.clone(),
            },
        );
        assert!(matches!(
            reducer.plan(&ungranted),
            Err(Error::Unauthorized(_))
        ));
        let mut unknown = command(
            operation(24),
            2,
            Action::ResolveInteraction {
                resolution: InteractionResolution {
                    id: ticket.id,
                    expected_version: 1,
                    outcome: InteractionOutcome::Indeterminate {
                        operation_id: operation(30),
                    },
                    detail: None,
                },
            },
        );
        unknown.scope = issuer().root(
            "responder",
            Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
        );
        assert!(matches!(
            reducer.apply(unknown)?,
            ApplyResult::Applied { .. }
        ));
        let mut granted = command(
            operation(21),
            3,
            Action::ResolveInteraction {
                resolution: resolution.clone(),
            },
        );
        granted.scope = issuer().root(
            "responder",
            Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
        );
        assert!(matches!(
            reducer.apply(granted.clone())?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            reducer.apply(granted)?,
            ApplyResult::Replayed { .. }
        ));
        assert_eq!(
            reducer
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref()),
            Some(&resolution)
        );
        let mut stale = command(operation(22), 4, Action::ResolveInteraction { resolution });
        stale.scope = issuer().root(
            "responder",
            Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
        );
        assert!(matches!(reducer.plan(&stale), Err(Error::Conflict(_))));
        let snapshot = reducer.snapshot()?;
        let restored = Reducer::restore(snapshot, issuer().verifier(), schemas())?;
        assert!(
            restored
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref())
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn exact_replay_is_idempotent_and_conflicting_reuse_is_rejected() -> Result<()> {
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        let append = command(
            operation(1),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        );
        assert!(matches!(
            reducer.apply(append.clone())?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            reducer.apply(append)?,
            ApplyResult::Replayed { .. }
        ));
        assert_eq!(reducer.revision(), 1);
        let conflicting = command(
            operation(1),
            1,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"different"}"#)?,
            },
        );
        assert!(matches!(
            reducer.apply(conflicting),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn extension_replay_requires_exact_implementation_and_fork_policy() -> Result<()> {
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let mut original = Reducer::new(authority.clone(), issuer().verifier(), schemas());
        let ApplyResult::Applied { event } = original.apply(command(
            operation(70),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        ))?
        else {
            return Err(Error::Conflict("first extension append replayed".into()));
        };
        let schema = json!({
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "additionalProperties": true
        });
        for (digest, policy) in [
            ([8; 32], ExtensionForkPolicy::Inherit),
            ([9; 32], ExtensionForkPolicy::Reset),
        ] {
            let mut bindings = SchemaRegistry::new();
            bindings.register("example.message", 1, schema.clone(), digest, policy)?;
            let mut restored = Reducer::new(authority.clone(), issuer().verifier(), bindings);
            assert!(matches!(
                restored.apply_committed(event.clone()),
                Err(Error::Conflict(_))
            ));
        }
        let mut missing = Reducer::new(
            authority.clone(),
            issuer().verifier(),
            SchemaRegistry::new(),
        );
        assert!(matches!(
            missing.apply_committed(event.clone()),
            Err(Error::Unsupported(_))
        ));
        let mut exact = Reducer::new(authority, issuer().verifier(), schemas());
        assert!(matches!(
            exact.apply_committed(event)?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            exact.schemas.register(
                "example.message",
                1,
                schema,
                [8; 32],
                ExtensionForkPolicy::Inherit
            ),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn fixed_lifecycle_rejects_terminal_reopening() -> Result<()> {
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Turn,
                id: "turn-1".into(),
            },
            AuthorityIssuer::new(
                "test",
                [7; 32],
                Authority {
                    kind: AggregateKind::Turn,
                    id: "turn-1".into(),
                },
            )
            .verifier(),
            schemas(),
        );
        let turn_issuer = AuthorityIssuer::new("test", [7; 32], reducer.authority().clone());
        let mut start = command(
            operation(40),
            0,
            Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            },
        );
        start.scope = turn_issuer.root("turn", Capabilities::new(["lifecycle:manage"]));
        reducer.apply(start)?;
        let mut finish = command(
            operation(41),
            1,
            Action::TransitionLifecycle {
                to: LifecycleState::Completed,
                reason: None,
            },
        );
        finish.scope = turn_issuer.root("turn", Capabilities::new(["lifecycle:manage"]));
        reducer.apply(finish)?;
        let mut reopen = command(
            operation(42),
            2,
            Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            },
        );
        reopen.scope = turn_issuer.root("turn", Capabilities::new(["lifecycle:manage"]));
        assert!(matches!(reducer.apply(reopen), Err(Error::Conflict(_))));
        Ok(())
    }

    #[test]
    fn effect_lifecycle_is_explicit() -> Result<()> {
        let effect_id = EffectId::from_bytes([9; 16]);
        let first_attempt = EffectAttemptId::from_bytes([10; 16]);
        let retry_attempt = EffectAttemptId::from_bytes([11; 16]);
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        reducer.apply(command(
            operation(1),
            0,
            Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee: EffectGuarantee::IdempotentRetry,
                effect_kind: "example.send".into(),
                request: request_file(br#"{"value":1}"#)?,
                result_schema: schema_file(&json!({"type": "object", "required": ["receipt"]}))?,
            },
        ))?;
        assert!(
            !serde_json::to_string(&reducer.snapshot()?)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .contains("\\\"value\\\"")
        );
        reducer.apply(command(
            operation(2),
            1,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id: first_attempt,
            },
        ))?;
        let indeterminate = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            first_attempt,
            EffectStatus::Indeterminate,
        )?;
        reducer.apply(command(
            operation(3),
            2,
            Action::ResolveEffect {
                observation: indeterminate,
            },
        ))?;
        assert_eq!(
            reducer.effect(effect_id).map(|effect| &effect.status),
            Some(&EffectStatus::Indeterminate)
        );
        reducer.apply(command(
            operation(4),
            3,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id: retry_attempt,
            },
        ))?;
        let succeeded = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            retry_attempt,
            EffectStatus::Succeeded {
                result: request_file(br#"{"receipt":"confirmed"}"#)?,
            },
        )?;
        reducer.apply(command(
            operation(5),
            4,
            Action::ResolveEffect {
                observation: succeeded,
            },
        ))?;
        assert!(matches!(
            reducer.effect(effect_id).map(|effect| &effect.status),
            Some(EffectStatus::Succeeded { .. })
        ));
        assert_eq!(
            reducer
                .effect(effect_id)
                .map(|effect| effect.attempts.len()),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn at_most_once_effect_is_never_redispatched_after_uncertainty() -> Result<()> {
        let effect_id = EffectId::from_bytes([20; 16]);
        let attempt_id = EffectAttemptId::from_bytes([21; 16]);
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        reducer.apply(command(
            operation(20),
            0,
            Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee: EffectGuarantee::AtMostOnce,
                effect_kind: "example.send".into(),
                request: request_file(b"null")?,
                result_schema: schema_file(&json!({}))?,
            },
        ))?;
        reducer.apply(command(
            operation(21),
            1,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            },
        ))?;
        let indeterminate = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            attempt_id,
            EffectStatus::Indeterminate,
        )?;
        reducer.apply(command(
            operation(22),
            2,
            Action::ResolveEffect {
                observation: indeterminate,
            },
        ))?;
        assert!(matches!(
            reducer.apply(command(
                operation(23),
                3,
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: EffectAttemptId::from_bytes([22; 16]),
                },
            )),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn forged_or_malformed_effect_observations_are_rejected() -> Result<()> {
        let effect_id = EffectId::from_bytes([30; 16]);
        let attempt_id = EffectAttemptId::from_bytes([31; 16]);
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        reducer.apply(command(
            operation(30),
            0,
            Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee: EffectGuarantee::ExactlyOnce,
                effect_kind: "example.send".into(),
                request: request_file(br#"{"value":1}"#)?,
                result_schema: schema_file(&json!({"type": "string"}))?,
            },
        ))?;
        reducer.apply(command(
            operation(31),
            1,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            },
        ))?;
        let effect = reducer
            .effect(effect_id)
            .ok_or_else(|| Error::NotFound("effect".into()))?;
        let invalid_result = issuer().attest_effect(
            effect_id,
            effect,
            attempt_id,
            EffectStatus::Succeeded {
                result: FileRef::new(
                    request_file(b"1")?.volume().clone(),
                    "results/invalid.txt",
                    "generation-1",
                    crate::conversation::FileDescriptor::from_bytes(b"1", "text/plain")?,
                    "invalid.txt",
                )?,
            },
        )?;
        assert!(matches!(
            reducer.apply(command(
                operation(32),
                2,
                Action::ResolveEffect {
                    observation: invalid_result,
                },
            )),
            Err(Error::Invalid(_))
        ));
        let mut forged = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            attempt_id,
            EffectStatus::Indeterminate,
        )?;
        forged.provider = "attacker".into();
        assert!(matches!(
            reducer.apply(command(
                operation(33),
                2,
                Action::ResolveEffect {
                    observation: forged,
                },
            )),
            Err(Error::Unauthorized(_))
        ));
        let unknown = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            attempt_id,
            EffectStatus::Indeterminate,
        )?;
        reducer.apply(command(
            operation(34),
            2,
            Action::ResolveEffect {
                observation: unknown,
            },
        ))?;
        assert!(matches!(
            reducer.apply(command(
                operation(35),
                3,
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: EffectAttemptId::from_bytes([32; 16]),
                },
            )),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn malformed_admission_metadata_is_rejected() -> Result<()> {
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        let mut empty_key = command(
            operation(30),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        );
        empty_key.idempotency_key = IdempotencyKey(String::new());
        assert!(matches!(reducer.apply(empty_key), Err(Error::Invalid(_))));

        let mut future_cause = command(
            operation(31),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        );
        future_cause.causal_parent = Some(EventReference {
            authority: reducer.authority().clone(),
            revision: 1,
        });
        assert!(matches!(
            reducer.apply(future_cause),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn planning_does_not_advance_authoritative_state() -> Result<()> {
        let reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        let planned = reducer.plan(&command(
            operation(1),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        ))?;
        assert!(matches!(planned, ApplyResult::Applied { .. }));
        assert_eq!(reducer.revision(), 0);
        Ok(())
    }

    #[test]
    fn fork_is_invisible_until_one_seed_event_commits() -> Result<()> {
        use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
        use crate::fork::{CapturedResource, ResourceRevision};
        use crate::resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef, StreamRef};

        let stream_provider = ProviderRef::new("acyclic", "stream", "1")?;
        let filesystem_provider = ProviderRef::new("acyclic", "filesystem", "2")?;
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-2".into(),
        };
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let child_agent = crate::AgentId::from_bytes([6; 16]);
        let child_project = VolumeRef::new(
            filesystem_provider.clone(),
            "child-project",
            VolumeClass::Project,
            VolumeOwner::Project("project-1".into()),
        )?;
        let seed = ForkSeed {
            operation_id: operation(8),
            parent: parent.clone(),
            parent_revision: 1,
            child: child.clone(),
            child_agent,
            resources: vec![
                CapturedResource {
                    source: ResourceRevision::History(StreamRef::new(
                        stream_provider,
                        parent.stream_path()?.into_bytes(),
                        Some("1".into()),
                    )?),
                    revision: ResourceRevision::History(StreamRef::new(
                        ProviderRef::new("acyclic", "stream", "1")?,
                        parent.stream_path()?.into_bytes(),
                        Some("1".into()),
                    )?),
                },
                CapturedResource {
                    source: ResourceRevision::Project {
                        volume: VolumeRef::new(
                            filesystem_provider.clone(),
                            "parent-project",
                            VolumeClass::Project,
                            VolumeOwner::Project("project-1".into()),
                        )?,
                        generation: GenerationRef::new(
                            filesystem_provider.clone(),
                            [1; 32],
                            Some("6".into()),
                        )?,
                    },
                    revision: ResourceRevision::Project {
                        volume: child_project,
                        generation: GenerationRef::new(
                            filesystem_provider.clone(),
                            [2; 32],
                            Some("7".into()),
                        )?,
                    },
                },
            ],
            omissions: Vec::new(),
            child_private_volume: VolumeRef::new(
                filesystem_provider.clone(),
                "child-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
            )?,
            child_private_generation: GenerationRef::new(filesystem_provider, [3; 32], None)?,
            inherited_context: Vec::new(),
            shared_grants: Vec::new(),
            attached_agents: Vec::new(),
            reference_grants: Vec::new(),
            attachment_manifests: Vec::new(),
            inherited_through_sequence: 0,
            boundary: None,
        };
        let extension = ResourceRevision::Extension {
            name: "example.message".into(),
            version: 1,
            implementation_digest: [9; 32],
            reference: ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test", "objects", "1")?,
                b"extension-state".to_vec(),
                Some("immutable-1".into()),
            )?,
        };
        let mut extension_seed = seed.clone();
        extension_seed.resources.push(CapturedResource {
            source: extension.clone(),
            revision: extension,
        });
        for (policy, allowed) in [
            (ExtensionForkPolicy::Inherit, true),
            (ExtensionForkPolicy::Reset, false),
            (ExtensionForkPolicy::Reject, false),
        ] {
            let mut bindings = SchemaRegistry::new();
            bindings.register(
                "example.message",
                1,
                json!({"type": "object"}),
                [9; 32],
                policy,
            )?;
            let mut candidate = Reducer::new(parent.clone(), issuer().verifier(), bindings);
            candidate.apply(command(
                operation(1),
                0,
                Action::BindConversation {
                    agent: crate::AgentId::from_bytes([1; 16]),
                },
            ))?;
            let result = candidate.plan(&command(
                operation(8),
                1,
                Action::PublishFork {
                    seed: Box::new(extension_seed.clone()),
                },
            ));
            assert_eq!(
                result.is_ok(),
                allowed,
                "fork policy {policy:?}: {result:?}"
            );
        }
        let mut reset_seed = extension_seed.clone();
        if let ResourceRevision::Extension { reference, .. } = &mut reset_seed.resources[2].revision
        {
            *reference = ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test", "objects", "1")?,
                b"child-extension-state".to_vec(),
                Some("immutable-1".into()),
            )?;
        }
        for (policy, allowed) in [
            (ExtensionForkPolicy::Inherit, false),
            (ExtensionForkPolicy::Reset, true),
        ] {
            let mut bindings = SchemaRegistry::new();
            bindings.register(
                "example.message",
                1,
                json!({"type": "object"}),
                [9; 32],
                policy,
            )?;
            let mut candidate = Reducer::new(parent.clone(), issuer().verifier(), bindings);
            candidate.apply(command(
                operation(1),
                0,
                Action::BindConversation {
                    agent: crate::AgentId::from_bytes([1; 16]),
                },
            ))?;
            let result = candidate.plan(&command(
                operation(8),
                1,
                Action::PublishFork {
                    seed: Box::new(reset_seed.clone()),
                },
            ));
            assert_eq!(
                result.is_ok(),
                allowed,
                "reset policy {policy:?}: {result:?}"
            );
        }
        let mut mismatched = extension_seed;
        if let ResourceRevision::Extension {
            implementation_digest,
            ..
        } = &mut mismatched.resources[2].revision
        {
            *implementation_digest = [8; 32];
        }
        mismatched.resources[2].source = mismatched.resources[2].revision.clone();
        let mut candidate = Reducer::new(parent.clone(), issuer().verifier(), schemas());
        candidate.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        assert!(matches!(
            candidate.plan(&command(
                operation(8),
                1,
                Action::PublishFork {
                    seed: Box::new(mismatched)
                },
            )),
            Err(Error::Conflict(_))
        ));
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        reducer.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        let planned = reducer.plan(&command(
            operation(8),
            1,
            Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        ))?;
        assert!(reducer.fork(&child).is_none());
        let ApplyResult::Applied { event } = planned else {
            return Err(Error::Invalid("new fork unexpectedly replayed".into()));
        };
        reducer.apply_committed(event)?;
        assert_eq!(reducer.fork(&child), Some(&seed));
        let mut reused_private = seed;
        reused_private.operation_id = operation(9);
        reused_private.child.id = "conversation-3".into();
        reused_private.parent_revision = 2;
        let next_history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic", "stream", "1")?,
            parent.stream_path()?.into_bytes(),
            Some("2".into()),
        )?);
        reused_private.resources[0].source = next_history.clone();
        reused_private.resources[0].revision = next_history;
        reused_private.validate()?;
        let duplicate = reducer.plan(&command(
            operation(9),
            2,
            Action::PublishFork {
                seed: Box::new(reused_private),
            },
        ));
        assert!(
            matches!(duplicate, Err(Error::Conflict(_))),
            "{duplicate:?}"
        );
        Ok(())
    }

    #[test]
    fn snapshot_restores_exact_projection_and_rejects_tampering() -> Result<()> {
        let verifier = issuer().verifier();
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            verifier.clone(),
            schemas(),
        );
        reducer.apply(command(
            operation(1),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"hello"}"#)?,
            },
        ))?;
        let snapshot = reducer.snapshot()?;
        assert_eq!(
            Reducer::restore(snapshot.clone(), verifier.clone(), schemas())?,
            reducer
        );
        let mut tampered = snapshot;
        tampered.revision = 2;
        assert!(matches!(
            Reducer::restore(tampered, verifier, schemas()),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn scope_attenuation_cannot_expand_authority() {
        let issuer = issuer();
        let root = issuer.root("root", Capabilities::new(["read", "write"]));
        assert!(
            issuer
                .attenuate(&root, "child", Capabilities::new(["read"]))
                .is_ok()
        );
        assert!(matches!(
            issuer.attenuate(&root, "child", Capabilities::new(["read", "admin"])),
            Err(Error::Unauthorized(_))
        ));
    }

    #[test]
    fn mutated_or_foreign_scopes_are_rejected() -> Result<()> {
        let trusted = issuer();
        let scope = trusted.root("root", Capabilities::new(["event:append"]));
        let foreign_authority = Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        };
        let foreign = AuthorityIssuer::new("test", [7; 32], foreign_authority.clone());
        let foreign_reducer = Reducer::new(foreign_authority, foreign.verifier(), schemas());
        let foreign_command = Command {
            operation_id: operation(1),
            idempotency_key: IdempotencyKey("foreign".into()),
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(b"{}")?,
            },
        };
        assert!(matches!(
            foreign_reducer.plan(&foreign_command),
            Err(Error::Unauthorized(_))
        ));
        let mut scope = scope;
        scope.capabilities = Capabilities::new(["event:append", "effect:run"]);
        let reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            trusted.verifier(),
            schemas(),
        );
        let forged = Command {
            operation_id: operation(1),
            idempotency_key: IdempotencyKey("forged".into()),
            expected_revision: 0,
            scope,
            causal_parent: None,
            action: Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(b"{}")?,
            },
        };
        assert!(matches!(reducer.plan(&forged), Err(Error::Unauthorized(_))));
        Ok(())
    }

    #[test]
    fn conversation_binding_and_messages_replay_through_snapshot() -> Result<()> {
        use crate::conversation::{
            FileDescriptor, FileRef, MessageKind, VolumeClass, VolumeOwner, VolumeRef,
        };
        use crate::resources::ProviderRef;

        let agent = crate::AgentId::from_bytes([9; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("acyclic", "filesystem", "2")?,
            "agent-9",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        let content = FileRef::new(
            volume,
            "messages/one.txt",
            "generation-1",
            FileDescriptor::from_bytes(b"hello", "text/plain")?,
            "one.txt",
        )?;
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let verifier = issuer().verifier();
        let mut reducer = Reducer::new(authority, verifier.clone(), schemas());
        assert!(matches!(
            reducer.plan(&command(
                operation(1),
                0,
                Action::AppendConversationMessage {
                    message: Box::new(crate::conversation::ConversationMessage {
                        id: uuid::Uuid::from_bytes([1; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: content.clone(),
                        attachments: Vec::new().into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                }
            )),
            Err(Error::Conflict(_))
        ));
        reducer.apply(command(operation(1), 0, Action::BindConversation { agent }))?;
        let message = crate::conversation::ConversationMessage {
            id: uuid::Uuid::from_bytes([1; 16]),
            sequence: 1,
            kind: MessageKind::User,
            content,
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let admitted = reducer.apply(command(
            operation(2),
            1,
            Action::AppendConversationMessage {
                message: Box::new(message.clone()),
            },
        ))?;
        if let ApplyResult::Applied { event } = admitted {
            let canonical =
                serde_json::to_string(&event).map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(
                !canonical.contains("hello"),
                "durable events must contain refs, not file bodies"
            );
        } else {
            return Err(Error::Invalid(
                "conversation append was not admitted".into(),
            ));
        }
        assert_eq!(
            reducer
                .conversation()
                .map(|state| state.messages.as_slice()),
            Some(&[message.clone()][..])
        );
        let selection = ModelContextSelection {
            conversation_revision: 1,
            message_ids: vec![message.id],
        };
        reducer.apply(command(
            operation(3),
            2,
            Action::SelectModelContext {
                selection: selection.clone(),
            },
        ))?;
        assert_eq!(reducer.context_selections(), &[selection]);
        assert!(matches!(
            reducer.plan(&command(
                operation(4),
                3,
                Action::SelectModelContext {
                    selection: ModelContextSelection {
                        conversation_revision: 0,
                        message_ids: vec![message.id]
                    },
                }
            )),
            Err(Error::Conflict(_))
        ));
        let restored = Reducer::restore(reducer.snapshot()?, verifier, schemas())?;
        assert_eq!(restored.conversation(), reducer.conversation());
        assert_eq!(restored.context_selections(), reducer.context_selections());
        Ok(())
    }
}
