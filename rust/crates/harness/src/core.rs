//! Deterministic, host-neutral durable substrate semantics.

use crate::{
    Capabilities, EffectAttemptId, EffectId, Error, IdempotencyKey, InteractionId, OperationId,
    PolicyLayer, Result,
    fork::ForkManifest,
    interaction::{Interaction, InteractionResponse},
    resolve_policy_layers,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
        Ok(format!("harness/{segment}/{}", self.id))
    }
}

/// Immutable authorization scope captured at admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Scope {
    /// Stable scope identity.
    id: String,
    /// Effective capabilities after policy intersection.
    capabilities: Capabilities,
    /// Trusted issuer identity.
    issuer: String,
    /// Parent proof, present for attenuated scopes.
    parent_proof: Option<[u8; 32]>,
    /// Keyed proof over the complete grant.
    proof: [u8; 32],
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

    #[cfg(any(feature = "host", feature = "wasm"))]
    pub(crate) fn wire_parts(&self) -> (&str, Option<[u8; 32]>, [u8; 32]) {
        (&self.issuer, self.parent_proof, self.proof)
    }

    #[cfg(any(feature = "host", feature = "wasm"))]
    pub(crate) fn from_wire(
        id: String,
        capabilities: Capabilities,
        issuer: String,
        parent_proof: Option<[u8; 32]>,
        proof: [u8; 32],
    ) -> Self {
        Self {
            id,
            capabilities,
            issuer,
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
        self.issue(id.into(), capabilities, None)
    }

    /// Issues a root grant after resolving all explicit hierarchy policy layers.
    pub fn root_with_policies(
        &self,
        id: impl Into<String>,
        layers: &[PolicyLayer],
    ) -> Result<Scope> {
        Ok(self.issue(id.into(), resolve_policy_layers(layers)?, None))
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
        Ok(self.issue(id.into(), capabilities, Some(parent.proof)))
    }

    fn issue(
        &self,
        id: String,
        capabilities: Capabilities,
        parent_proof: Option<[u8; 32]>,
    ) -> Scope {
        let proof = scope_proof(
            &self.key,
            &self.audience,
            &self.id,
            &id,
            &capabilities,
            parent_proof,
        );
        Scope {
            id,
            capabilities,
            issuer: self.id.clone(),
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityVerifier {
    id: String,
    key: [u8; 32],
    audience: Authority,
}

impl AuthorityVerifier {
    /// Verifies issuer identity and the complete immutable grant.
    pub fn verify(&self, scope: &Scope) -> Result<()> {
        let expected = scope_proof(
            &self.key,
            &self.audience,
            &scope.issuer,
            &scope.id,
            &scope.capabilities,
            scope.parent_proof,
        );
        if scope.issuer != self.id || scope.proof != expected {
            return Err(Error::Unauthorized("scope proof is invalid".into()));
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
pub enum EffectStatus {
    /// Effect is durable but has not been dispatched.
    Planned,
    /// Dispatch began and its external outcome may still be unknown.
    Dispatched,
    /// Effect completed successfully.
    Succeeded {
        /// Schema-defined result.
        result: Value,
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
    /// Immutable provider request.
    pub request: Value,
    /// Canonical digest binding provider, kind, guarantee, and request.
    pub request_digest: [u8; 32],
    /// Immutable JSON Schema for successful results.
    pub result_schema: Value,
    /// Digest pinning the result schema.
    pub result_schema_digest: [u8; 32],
    /// Current semantic completion state.
    pub status: EffectStatus,
    /// Ordered unique attempts made against the provider.
    pub attempts: Vec<EffectAttemptId>,
}

/// Schema-validated event payload retained in canonical history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
        /// Namespaced schema identity.
        schema: String,
        /// Positive schema version.
        version: u32,
        /// Digest pinning the exact registered schema.
        schema_digest: [u8; 32],
        /// Schema-defined payload.
        value: Value,
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
        /// Schema-defined provider request.
        request: Value,
        /// Canonical digest of the complete provider request identity.
        request_digest: [u8; 32],
        /// JSON Schema for a successful provider result.
        result_schema: Value,
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
    /// Durable typed interaction became available to participants.
    InteractionOpened {
        /// Stable interaction identity.
        interaction_id: InteractionId,
        /// Typed request.
        interaction: Interaction,
    },
    /// Durable single response to an open interaction.
    InteractionResolved {
        /// Stable interaction identity.
        interaction_id: InteractionId,
        /// Validated typed response.
        response: InteractionResponse,
    },
    /// Prepared immutable environment references became atomically visible.
    ForkPublished {
        /// Complete child publication record.
        manifest: Box<ForkManifest>,
    },
}

/// Reference to an exact event in another aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventReference {
    /// Referenced authority.
    pub authority: Authority,
    /// Exact referenced revision.
    pub revision: u64,
}

/// One canonical event in an aggregate history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Gapless one-based aggregate revision.
    pub revision: u64,
    /// Operation that caused this event.
    pub operation_id: OperationId,
    /// Digest that permanently binds the operation identity to its command.
    pub intent_digest: [u8; 32],
    /// Complete authorization scope captured at admission.
    pub scope: Scope,
    /// Optional causal predecessor in another aggregate.
    pub causal_parent: Option<EventReference>,
    /// Typed payload.
    pub payload: EventPayload,
}

/// Immutable command envelope accepted by the reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[serde(tag = "kind", rename_all = "snake_case")]
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
        /// Schema-defined payload.
        value: Value,
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
        /// Schema-defined provider request.
        request: Value,
        /// JSON Schema for a successful provider result.
        result_schema: Value,
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
    /// Opens one typed interaction.
    OpenInteraction {
        /// Stable interaction identity.
        interaction_id: InteractionId,
        /// Typed request.
        interaction: Interaction,
    },
    /// Resolves one typed interaction exactly once.
    ResolveInteraction {
        /// Stable interaction identity.
        interaction_id: InteractionId,
        /// Typed response.
        response: InteractionResponse,
    },
    /// Atomically publishes one fully prepared child environment.
    PublishFork {
        /// Immutable references prepared outside the reducer.
        manifest: Box<ForkManifest>,
    },
}

/// Current projection of one durable interaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InteractionState {
    /// Original typed request.
    pub interaction: Interaction,
    /// Single validated response, or `None` while open.
    pub response: Option<InteractionResponse>,
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
pub struct Snapshot {
    /// Snapshot format version.
    pub format_version: u32,
    /// Aggregate represented by the snapshot.
    pub authority: Authority,
    /// Last included event revision.
    pub revision: u64,
    /// Canonical events included in this portable v1 snapshot.
    pub events: Vec<Event>,
    /// Digest over all preceding fields.
    pub state_digest: [u8; 32],
}

/// Immutable-version registry for namespaced extension payload schemas.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchemaRegistry {
    schemas: BTreeMap<(String, u32), (Value, [u8; 32])>,
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
    pub fn register(&mut self, name: impl Into<String>, version: u32, schema: Value) -> Result<()> {
        let name = name.into();
        if !name.contains('.') || name.starts_with("acyclic.") || version == 0 {
            return Err(Error::Invalid(
                "extension schema requires a non-reserved namespaced name and positive version"
                    .into(),
            ));
        }
        jsonschema::validator_for(&schema)
            .map_err(|error| Error::Invalid(format!("invalid JSON Schema: {error}")))?;
        let digest = json_digest(&schema)?;
        let key = (name, version);
        if let Some((existing, _)) = self.schemas.get(&key) {
            return if existing == &schema {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "schema identity is already pinned to another definition".into(),
                ))
            };
        }
        self.schemas.insert(key, (schema, digest));
        Ok(())
    }

    fn validate(
        &self,
        name: &str,
        version: u32,
        digest: Option<[u8; 32]>,
        value: &Value,
    ) -> Result<[u8; 32]> {
        let (schema, registered_digest) = self
            .schemas
            .get(&(name.to_owned(), version))
            .ok_or_else(|| Error::Unsupported(format!("extension schema {name}@{version}")))?;
        if digest.is_some_and(|actual| actual != *registered_digest) {
            return Err(Error::Conflict("extension schema digest mismatch".into()));
        }
        jsonschema::validator_for(schema)
            .map_err(|error| Error::Invalid(format!("invalid registered JSON Schema: {error}")))?
            .validate(value)
            .map_err(|error| {
                Error::Invalid(format!("extension payload failed validation: {error}"))
            })?;
        Ok(*registered_digest)
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
    interactions: BTreeMap<InteractionId, InteractionState>,
    forks: BTreeMap<Authority, ForkManifest>,
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
            interactions: BTreeMap::new(),
            forks: BTreeMap::new(),
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

    /// Returns the fixed aggregate lifecycle projection.
    #[must_use]
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Applies one deterministic command.
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
            _ => {}
        }
        if let Action::PublishFork { manifest } = &command.action
            && (manifest.validate().is_err()
                || manifest.operation_id != command.operation_id
                || manifest.parent != self.authority
                || manifest.parent_revision != self.revision)
        {
            return Err(Error::Invalid(
                "fork manifest is not bound to its command and parent revision".into(),
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
        let event = Event {
            revision,
            operation_id: command.operation_id,
            intent_digest: intent,
            scope: command.scope.clone(),
            causal_parent: command.causal_parent.clone(),
            payload,
        };
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
        self.authority_verifier.verify(&event.scope)?;
        require_capability(&event.scope, event.payload.required_capability())?;
        match &event.payload {
            EventPayload::EffectPlanned { .. } => {
                require_capability(&event.scope, "effect:plan")?;
            }
            EventPayload::EffectDispatched { effect_id, .. } => {
                let effect = self
                    .effects
                    .get(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                require_capability(
                    &event.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .effects
                    .get(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                require_capability(
                    &event.scope,
                    &format!("effect:provider:{}", effect.provider),
                )?;
            }
            _ => {}
        }
        validate_causal_parent(&self.authority, self.revision, event.causal_parent.as_ref())?;
        if let EventPayload::ForkPublished { manifest } = &event.payload
            && (manifest.validate().is_err()
                || manifest.operation_id != event.operation_id
                || manifest.parent != self.authority
                || manifest.parent_revision != self.revision)
        {
            return Err(Error::Invalid("fork event binding is invalid".into()));
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

    /// Returns an interaction's current projection.
    #[must_use]
    pub fn interaction(&self, interaction_id: InteractionId) -> Option<&InteractionState> {
        self.interactions.get(&interaction_id)
    }

    /// Returns a child only after its publication event committed.
    #[must_use]
    pub fn fork(&self, child: &Authority) -> Option<&ForkManifest> {
        self.forks.get(child)
    }

    /// Creates a portable, integrity-checked restoration accelerator.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot {
            format_version: 1,
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
        if snapshot.format_version != 1 {
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
                value,
            } => {
                let schema_digest = self.schemas.validate(schema, *version, None, value)?;
                Ok(EventPayload::Custom {
                    schema: schema.clone(),
                    version: *version,
                    schema_digest,
                    value: value.clone(),
                })
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
                let request_digest =
                    effect_request_digest(provider, *guarantee, effect_kind, request)?;
                jsonschema::validator_for(result_schema).map_err(|error| {
                    Error::Invalid(format!("invalid effect result schema: {error}"))
                })?;
                let result_schema_digest = json_digest(result_schema)?;
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
                validate_effect_result(&effect.result_schema, &observation.status)?;
                Ok(EventPayload::EffectResolved {
                    observation: observation.clone(),
                })
            }
            Action::OpenInteraction {
                interaction_id,
                interaction,
            } => {
                if self.interactions.contains_key(interaction_id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                interaction.validate()?;
                Ok(EventPayload::InteractionOpened {
                    interaction_id: *interaction_id,
                    interaction: interaction.clone(),
                })
            }
            Action::ResolveInteraction {
                interaction_id,
                response,
            } => {
                let state = self
                    .interactions
                    .get(interaction_id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {interaction_id}")))?;
                if state.response.is_some() {
                    return Err(Error::Conflict("interaction is already resolved".into()));
                }
                state.interaction.validate_response(response)?;
                Ok(EventPayload::InteractionResolved {
                    interaction_id: *interaction_id,
                    response: response.clone(),
                })
            }
            Action::PublishFork { manifest } => {
                manifest.validate()?;
                if self.forks.contains_key(&manifest.child) {
                    return Err(Error::Conflict("fork child is already published".into()));
                }
                Ok(EventPayload::ForkPublished {
                    manifest: manifest.clone(),
                })
            }
        }
    }

    fn apply_payload(&mut self, payload: &EventPayload) -> Result<()> {
        match payload {
            EventPayload::LifecycleTransitioned { from, to, .. } => {
                if *from != self.lifecycle {
                    return Err(Error::Conflict("lifecycle predecessor mismatch".into()));
                }
                validate_lifecycle(*from, *to)?;
                self.lifecycle = *to;
            }
            EventPayload::Custom {
                schema,
                version,
                schema_digest,
                value,
            } => {
                self.schemas
                    .validate(schema, *version, Some(*schema_digest), value)?;
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
                if self.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if *request_digest
                    != effect_request_digest(provider, *guarantee, effect_kind, request)?
                {
                    return Err(Error::Invalid("effect request digest mismatch".into()));
                }
                jsonschema::validator_for(result_schema).map_err(|error| {
                    Error::Invalid(format!("invalid effect result schema: {error}"))
                })?;
                if *result_schema_digest != json_digest(result_schema)? {
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
                validate_effect_result(&effect.result_schema, &observation.status)?;
                effect.status = observation.status.clone();
            }
            EventPayload::InteractionOpened {
                interaction_id,
                interaction,
            } => {
                interaction.validate()?;
                if self.interactions.contains_key(interaction_id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                self.interactions.insert(
                    *interaction_id,
                    InteractionState {
                        interaction: interaction.clone(),
                        response: None,
                    },
                );
            }
            EventPayload::InteractionResolved {
                interaction_id,
                response,
            } => {
                let state = self
                    .interactions
                    .get_mut(interaction_id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {interaction_id}")))?;
                if state.response.is_some() {
                    return Err(Error::Conflict("interaction is already resolved".into()));
                }
                state.interaction.validate_response(response)?;
                state.response = Some(response.clone());
            }
            EventPayload::ForkPublished { manifest } => {
                manifest.validate()?;
                if manifest.child == self.authority || self.forks.contains_key(&manifest.child) {
                    return Err(Error::Conflict("invalid fork publication".into()));
                }
                self.forks
                    .insert(manifest.child.clone(), manifest.as_ref().clone());
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
            Self::OpenInteraction { .. } => "interaction:open",
            Self::ResolveInteraction { .. } => "interaction:respond",
            Self::PublishFork { .. } => "fork:publish",
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
            Self::InteractionOpened { .. } => "interaction:open",
            Self::InteractionResolved { .. } => "interaction:respond",
            Self::ForkPublished { .. } => "fork:publish",
        }
    }
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
    let bytes = serde_json::to_vec(&(
        audience,
        provider,
        effect_id,
        attempt_id,
        request_digest,
        guarantee,
        result_schema_digest,
        status,
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::keyed_hash(key, &bytes).as_bytes())
}

pub(crate) fn canonical_intent(command: &Command) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(command).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn effect_request_digest(
    provider: &str,
    guarantee: EffectGuarantee,
    effect_kind: &str,
    request: &Value,
) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(&(provider, guarantee, effect_kind, request))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn validate_effect_result(schema: &Value, status: &EffectStatus) -> Result<()> {
    if let EffectStatus::Succeeded { result } = status {
        jsonschema::validator_for(schema)
            .map_err(|error| Error::Invalid(format!("invalid effect result schema: {error}")))?
            .validate(result)
            .map_err(|error| Error::Invalid(format!("effect result failed validation: {error}")))?;
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
    let bytes = serde_json::to_vec(&(
        snapshot.format_version,
        &snapshot.authority,
        snapshot.revision,
        &snapshot.events,
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn json_digest(value: &Value) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn operation(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
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
                ]),
            ),
            causal_parent: None,
            action,
        }
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
                value: json!({"text": "hello"}),
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
                value: json!({"text": "different"}),
            },
        );
        assert!(matches!(
            reducer.apply(conflicting),
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
                request: json!({"value": 1}),
                result_schema: json!({"type": "object", "required": ["receipt"]}),
            },
        ))?;
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
                result: json!({"receipt": "confirmed"}),
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
                request: Value::Null,
                result_schema: json!({}),
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
    fn forged_or_schema_invalid_effect_observations_are_rejected() -> Result<()> {
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
                request: json!({"value": 1}),
                result_schema: json!({"type": "string"}),
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
            EffectStatus::Succeeded { result: json!(1) },
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
                value: json!({"text": "hello"}),
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
                value: json!({"text": "hello"}),
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
                value: json!({"text": "hello"}),
            },
        ))?;
        assert!(matches!(planned, ApplyResult::Applied { .. }));
        assert_eq!(reducer.revision(), 0);
        Ok(())
    }

    #[test]
    fn fork_is_invisible_until_one_manifest_event_commits() -> Result<()> {
        use crate::resources::{GenerationRef, ProviderRef, StreamRef};

        let stream_provider = ProviderRef::new("acyclic", "stream", "1")?;
        let filesystem_provider = ProviderRef::new("acyclic", "filesystem", "2")?;
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-2".into(),
        };
        let manifest = ForkManifest {
            operation_id: operation(8),
            parent: Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            parent_revision: 0,
            child: child.clone(),
            stream: StreamRef::new(stream_provider, [1; 16], Some("3".into()))?,
            workspace: GenerationRef::new(filesystem_provider, [2; 32], Some("7".into()))?,
            machine: None,
        };
        let mut reducer = Reducer::new(
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
            issuer().verifier(),
            schemas(),
        );
        let planned = reducer.plan(&command(
            operation(8),
            0,
            Action::PublishFork {
                manifest: Box::new(manifest.clone()),
            },
        ))?;
        assert!(reducer.fork(&child).is_none());
        let ApplyResult::Applied { event } = planned else {
            return Err(Error::Invalid("new fork unexpectedly replayed".into()));
        };
        reducer.apply_committed(event)?;
        assert_eq!(reducer.fork(&child), Some(&manifest));
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
                value: json!({"text": "hello"}),
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
                value: json!({}),
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
                value: json!({}),
            },
        };
        assert!(matches!(reducer.plan(&forged), Err(Error::Unauthorized(_))));
        Ok(())
    }

    #[test]
    fn typed_approval_is_bound_and_resolved_once() -> Result<()> {
        let interaction_id = InteractionId::from_bytes([8; 16]);
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
            Action::OpenInteraction {
                interaction_id,
                interaction: Interaction::approval("Allow send?", operation(9), [4; 32])?,
            },
        ))?;
        assert!(matches!(
            reducer.plan(&command(
                operation(2),
                1,
                Action::ResolveInteraction {
                    interaction_id,
                    response: InteractionResponse::Question { value: json!(true) },
                },
            )),
            Err(Error::Invalid(_))
        ));
        reducer.apply(command(
            operation(3),
            1,
            Action::ResolveInteraction {
                interaction_id,
                response: InteractionResponse::Approval {
                    approved: true,
                    reason: None,
                },
            },
        ))?;
        assert!(matches!(
            reducer.interaction(interaction_id),
            Some(InteractionState {
                response: Some(InteractionResponse::Approval { approved: true, .. }),
                ..
            })
        ));
        assert!(matches!(
            reducer.apply(command(
                operation(4),
                2,
                Action::ResolveInteraction {
                    interaction_id,
                    response: InteractionResponse::Approval {
                        approved: false,
                        reason: None,
                    },
                },
            )),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}
