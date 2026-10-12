//! Deterministic, host-neutral durable substrate semantics.

use crate::contract::capability;
use crate::contract::next_revision;
use crate::store::history::HistoryIo;
use crate::{
    AgentId, Capabilities, EffectAttemptId, EffectId, Error, IdempotencyKey, OperationId,
    Result,
    conversation::{
        ContentGrant, ConversationMessage, ConversationState, FileRef,
        ModelContextSelection, VolumeOperation, VolumeOwner, VolumeRef, is_internal_path,
    },
    fork::{ForkSeed, InheritedConversationPrefix},
    interaction::{InteractionOutcome, InteractionResolution, InteractionTicket},
    merge::ProjectMergeReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

mod snapshot;
mod native_custody;
use native_custody::{NativeComponentCustody, NativeComponentKey};
pub use snapshot::Snapshot;
pub(crate) use snapshot::{
    source_inventory,
    source_metadata_inspection_with_work,
};
pub(crate) use snapshot::{
    resource_creation_proof_clone_with_custody,
    resource_preparation_owned_bytes,
};
#[cfg(feature = "filesystem")]
pub(crate) use snapshot::resource_creation_clone_reservation;
pub(crate) use native_custody::{
    physical_clone_factory,
    copy_iter, copy_copy_vector, reserve_copy_vector, copy_text, copy_authority, copy_provider,
    copy_file, copy_vector, copy_boxed, copy_configuration, copy_value, copy_capabilities,
    copy_reference,
    native_effect_clone_with_custody,
    native_interaction_resolution_clone_with_custody, native_interaction_ticket_clone_with_custody,
    native_interaction_outcome_clone_with_custody,
    native_value_clone_with_custody, native_event_clone_with_custody,
    event_owned_bytes, value_owned_bytes,
};

/// Kind of independently ordered durable aggregate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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

/// Exact aggregate identities that cannot name a Stream path segment.
pub const AUTHORITY_ID_FORBIDDEN_EXACT: [&str; 2] = [".", ".."];

/// Separators that cannot appear in an aggregate identity.
pub const AUTHORITY_ID_FORBIDDEN_SEPARATORS: [char; 2] = ['/', '\\'];

/// Stable identity of one independently ordered history.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Authority {
    /// Aggregate kind.
    pub kind: AggregateKind,
    /// Provider-independent identity.
    pub id: String,
}

impl Authority {
    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.id.is_empty()
            || AUTHORITY_ID_FORBIDDEN_EXACT.contains(&self.id.as_str())
            || self.id.contains(AUTHORITY_ID_FORBIDDEN_SEPARATORS)
            || self.id.chars().any(char::is_control)
        {
            return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Invalid,
                "aggregate identity is not a safe path segment"));
        }
        Ok(())
    }

    /// Returns the Stream path for this aggregate.
    pub fn stream_path(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        self.validate_with_work(work)?;
        let segment = match self.kind {
            AggregateKind::Agent => "agents",
            AggregateKind::Conversation => "conversations",
            AggregateKind::Session => "sessions",
            AggregateKind::Turn => "turns",
            AggregateKind::Task => "tasks",
        };
        if let Some(work) = work.as_deref_mut() {
            let bytes = "harness/v2/".len().checked_add(segment.len())
                .and_then(|bytes| bytes.checked_add(1))
                .and_then(|bytes| bytes.checked_add(self.id.len()))
                .ok_or_else(|| Error::Invalid("aggregate path length overflows".into()))?;
            let mut path = String::new();
            work.reserve_text(&mut path, bytes)?;
            work.copy_bytes(bytes as u64)?;
            path.push_str("harness/v2/");
            path.push_str(segment);
            path.push('/');
            path.push_str(&self.id);
            Ok(path)
        } else {
            Ok(format!("harness/v2/{segment}/{}", self.id))
        }
    }
}

/// Immutable authorization scope captured at admission.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Stable scope identity.
    id: String,
    /// Effective capabilities after policy intersection.
    capabilities: Capabilities,
    /// Host-attested agent on whose behalf this operation runs, if any.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    agent: Option<AgentId>,
    /// Trusted issuer identity.
    issuer: String,
    /// Parent proof, present for attenuated scopes.
    #[cfg_attr(feature = "wasm", tsify(type = "number[] | null"))]
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

    /// Copies the original immutable grant on the caller's admitted ledger.
    pub fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() {
            return Ok(self.clone());
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let cloned = self.copy_with_quota(&mut custody)?;
        custody.transfer();
        Ok(cloned)
    }

    pub(crate) fn copy_with_quota(
        &self, custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if custody.work().is_none() { return Ok(self.clone()); }
        let id = native_custody::copy_text(&self.id, custody)?;
        let issuer = native_custody::copy_text(&self.issuer, custody)?;
        let capabilities = native_custody::copy_capabilities(&self.capabilities, custody)?;
        Ok(Self { id, capabilities, agent: self.agent, issuer,
            parent_proof: self.parent_proof, proof: self.proof })
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let capabilities = self.capabilities.logical_owned_bytes()?;
        (self.id.capacity() as u64).checked_add(self.issuer.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(capabilities))
            .ok_or_else(|| Error::Invalid("scope owned byte count overflows".into()))
    }

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

    /// Copies the original issuer metadata under the caller's original ledger.
    pub fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let cloned = (|| -> Result<Self> {
            let id = quota.work().as_deref_mut().expect("issuer clone observer").clone_text(&self.id)?;
            quota.adopt(id.capacity() as u64)?;
            let audience = quota.work().as_deref_mut().expect("issuer clone observer").clone_authority(&self.audience)?;
            quota.adopt(audience.id.capacity() as u64)?;
            Ok(Self { id, key: self.key, audience })
        })()?;
        quota.transfer();
        Ok(cloned)
    }
    /// Borrows the original configured issuer identity without exposing its key.
    #[must_use]
    pub fn id(&self) -> &str { &self.id }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn audience(&self) -> &Authority { &self.audience }

    /// Copies the configured verifier using the caller's ledger.
    pub fn verifier(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        match work.as_deref_mut() {
            Some(work) => Ok(AuthorityVerifier {
                id: work.clone_text(&self.id)?,
                key: self.key,
                audience: work.clone_authority(&self.audience)?,
            }),
            None => Ok(AuthorityVerifier {
                id: self.id.clone(), key: self.key, audience: self.audience.clone(),
            }),
        }
    }

    /// Signs one grant using the caller's optional ledger.
    pub fn issue(
        &self, id: &str, capabilities: &Capabilities, agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Scope> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let id = native_custody::copy_text(id, &mut quota)?;
        let capabilities = native_custody::copy_capabilities(capabilities, &mut quota)?;
        let issuer = native_custody::copy_text(&self.id, &mut quota)?;
        let proof = scope_proof(
            &self.key, &self.audience, &self.id, &id, &capabilities, agent, parent_proof, quota.work(),
        )?;
        quota.transfer();
        Ok(Scope { id, capabilities, issuer, agent, parent_proof, proof })
    }

    /// Issues a child grant that cannot widen its parent.
    pub fn attenuate(
        &self,
        parent: &Scope,
        id: impl Into<String>,
        capabilities: Capabilities,
    ) -> Result<Scope> {
        self.verifier(&mut None).expect("verifier copy").verify(parent, &mut None)?;
        if !capabilities.is_subset_of(&parent.capabilities) {
            return Err(Error::Unauthorized(
                "child scope cannot widen ancestor capabilities".into(),
            ));
        }
        self.issue(&id.into(), &capabilities, parent.agent, Some(parent.proof), &mut None)
    }

    /// Delegates one pinned private file to another agent without a fork or directory-wide grant.
    pub fn delegate_private_file_read(
        &self,
        owner_scope: &Scope,
        reader: AgentId,
        id: impl Into<String>,
        file: &FileRef,
    ) -> Result<Scope> {
        file.validate(&mut None)?;
        if is_internal_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage files cannot be delegated".into(),
            ));
        }
        let volume = file.volume();
        self.require_private_owner_read(owner_scope, volume)?;
        self.issue(
            &id.into(), &Capabilities::new([file.read_capability()?]),
            Some(reader), Some(owner_scope.proof), &mut None,
        )
    }

    /// Shares a private directory lazily with an agent outside any fork tree.
    pub fn delegate_private_directory_read(
        &self,
        owner_scope: &Scope,
        reader: AgentId,
        id: impl Into<String>,
        volume: &crate::conversation::VolumeRef,
        prefix: &str,
    ) -> Result<Scope> {
        self.require_private_owner_read(owner_scope, volume)?;
        self.issue(
            &id.into(), &Capabilities::new([volume.directory_read_capability(prefix, &mut None)?]),
            Some(reader), Some(owner_scope.proof), &mut None,
        )
    }

    fn require_private_owner_read(
        &self,
        owner_scope: &Scope,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<()> {
        self.verifier(&mut None).expect("verifier copy").verify(owner_scope, &mut None)?;
        volume.validate(&mut None)?;
        if !matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == owner_scope.agent)
            || !owner_scope
                .capabilities
                .contains(&volume.capability(VolumeOperation::Read, &mut None)?)
        {
            return Err(Error::Unauthorized(
                "private read delegation requires its original owner".into(),
            ));
        }
        Ok(())
    }

    /// Signs the original provider observation with admitted encoding and hashing.
    pub(crate) fn attest_effect(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        attempt_id: EffectAttemptId,
        status: EffectStatus,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let provider = match custody.work().as_deref_mut() {
            Some(work) => work.clone_text(&effect.provider)?,
            None => effect.provider.clone(),
        };
        custody.adopt(provider.capacity() as u64)?;
        let proof = effect_attestation_proof_with_work(
            &self.key,
            &self.audience,
            &effect.provider,
            effect_id,
            attempt_id,
            effect.request_digest,
            effect.guarantee,
            effect.result_schema_digest,
            &status,
            custody.work(),
        )?;
        custody.transfer();
        Ok(EffectAttestation {
            provider,
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

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        (self.id.capacity() as u64).checked_add(self.audience.id.capacity() as u64)
            .ok_or_else(|| Error::Invalid("verifier owned byte count overflows".into()))
    }

    /// Copies the verifier using the caller's ledger.
    pub fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        match work.as_deref_mut() {
            Some(work) => Ok(Self {
                id: work.clone_text(&self.id)?,
                key: self.key,
                audience: work.clone_authority(&self.audience)?,
            }),
            None => Ok(self.clone()),
        }
    }

    /// Verifies issuer identity and the complete immutable grant.
    pub fn verify(
        &self, scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.verify_scope_for_with_work(&self.audience, scope, work)
    }

    /// Rebinds only the authenticated audience, retaining this original key.
    pub(crate) fn verify_scope_for_with_work(
        &self, audience: &Authority, scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let expected = scope_proof(
            &self.key, audience, &scope.issuer, &scope.id,
            &scope.capabilities, scope.agent, scope.parent_proof, work)?;
        if scope.issuer != self.id || scope.proof != expected {
            return Err(Error::Unauthorized("scope proof is invalid".into()));
        }
        Ok(())
    }

    fn canonical_proof<T: Serialize>(
        &self, domain: &[u8], value: &T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            value, crate::store::default_projection_read_limits().maximum_bytes, quota.work())?;
        quota.adopt(bytes.capacity() as u64)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(domain.len() as u64 + 8 + bytes.len() as u64)?;
        }
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(domain);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
        let proof = *hasher.finalize().as_bytes();
        drop(bytes);
        Ok(proof)
    }

    fn attest_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.canonical_proof(b"harness/v2/committed-event\0", &(
            &self.audience, event.revision, event.operation_id, event.intent_digest,
            &event.scope, &event.causal_parent, &event.payload,
        ), work)
    }

    pub(crate) fn verify_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if event.scope.issuer != self.id || event.attestation != self.attest_event(event, work)? {
            return Err(Error::Unauthorized(
                "event admission attestation is invalid".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn effect_history_root_proof_with_work(
        &self, revision: u64, canonical_digest: [u8; 32], root: Option<[u8; 32]>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let path = self.audience.stream_path(quota.work())?;
        quota.adopt(path.capacity() as u64)?;
        let proof = self.canonical_proof(b"harness/v1/effect-history-root\0", &(
            &self.id, &self.audience, &path, revision, canonical_digest, root,
        ), quota.work());
        drop(path);
        proof
    }

    pub(crate) fn fork_history_proof(
        &self, seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.verify_audience(&seed.parent, work)?;
        self.canonical_proof(b"harness/v1/fork-history-witness\0", &(
            (&self.id, &seed.operation_id, &seed.parent, seed.parent_revision,
             &seed.child, seed.child_agent, &seed.attached_agents, &seed.resources,
             &seed.omissions, &seed.child_private_volume),
            (&seed.child_private_generation, &seed.inherited_context, &seed.summary,
             seed.inherited_through_sequence, &seed.shared_grants,
             &seed.reference_grants, &seed.attachment_manifests, &seed.boundary),
        ), work)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn fork_history_selection_proof_with_work(
        &self, revision: u64, through_sequence: u64, agent: Option<AgentId>,
        history_digest: [u8; 32], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.canonical_proof(b"harness/v1/fork-history-selection\0", &(
            &self.id, &self.audience, revision, through_sequence, agent, history_digest,
        ), work)
    }

    /// Verifies the configured aggregate audience.
    pub fn verify_audience(
        &self, authority: &Authority,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() { io.check_cancellation()?; }
        if &self.audience == authority { return Ok(()); }
        Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Unauthorized,
            "authority verifier belongs to another aggregate"))
    }

    fn verify_effect(
        &self, observation: &EffectAttestation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let expected = effect_attestation_proof_with_work(
            &self.key,
            &self.audience,
            &observation.provider,
            observation.effect_id,
            observation.attempt_id,
            observation.request_digest,
            observation.guarantee,
            observation.result_schema_digest,
            &observation.status,
            work,
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
#[allow(clippy::large_enum_variant, reason = "public ref-valued status keeps its stable serde and Rust shape")]
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

impl EffectAttestation {
    physical_clone_factory!(copy, EffectAttestation, native_custody::copy_observation, self);
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

pub(super) fn settled_effect(status: &EffectStatus) -> bool {
    matches!(
        status,
        EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. }
    )
}

pub(crate) fn event_effect_id(payload: &EventPayload) -> Option<EffectId> {
    match payload {
        EventPayload::EffectPlanned { effect_id, .. }
        | EventPayload::EffectDispatched { effect_id, .. } => Some(*effect_id),
        EventPayload::EffectResolved { observation } => Some(observation.effect_id),
        _ => None,
    }
}

/// Schema-validated event payload retained in canonical history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant, reason = "canonical events prioritize typed fields over enum stack size")]
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
    id: String,
    capabilities: Capabilities,
    issuer: String,
    agent: Option<AgentId>,
}

impl RecordedScope {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let capabilities = self.capabilities.logical_owned_bytes()?;
        (self.id.capacity() as u64).checked_add(self.issuer.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(capabilities))
            .ok_or_else(|| Error::Invalid("recorded scope ownership overflows".into()))
    }

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

/// Version transition that never rewrites or removes its retained input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionStateMigration {
    /// Latest state event that was validated as the migration input.
    pub previous: EventReference,
    /// New schema-validated state revision.
    pub record: ExtensionRecord,
}

impl ExtensionStateMigration {
    /// Checks the portable envelope; the reducer additionally checks that `previous` is the latest state event under the same authority.
    pub fn validate(&self) -> Result<()> {
        if self.previous.revision == 0 {
            return Err(Error::Invalid("extension migration source is empty".into()));
        }
        self.previous.authority.stream_path(&mut None)?;
        self.record.validate()
    }
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
        self.content.validate(&mut None)
    }
}

fn extension_name_valid(name: &str) -> bool {
    name.contains('.')
        && !name.starts_with("acyclic.")
        && !name.split('.').any(str::is_empty)
        && name.chars().all(|character| character.is_alphanumeric() || matches!(character, '.' | '_' | '-'))
}

pub(crate) fn validate_extension_name(name: &str) -> Result<()> {
    if !extension_name_valid(name) {
        return Err(Error::Invalid(
            "extension name must be a bounded namespace".into(),
        ));
    }
    Ok(())
}

/// Exact installed version required by another extension at activation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionDependency {
    /// Namespaced extension identity.
    pub name: String,
    /// Required positive schema and implementation version.
    pub version: u32,
}

impl ExtensionDependency {
    /// Rejects ambiguous or unbounded dependency identities.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0 {
            return Err(Error::Invalid(
                "extension dependency version is zero".into(),
            ));
        }
        Ok(())
    }
}

/// File-backed configuration pinned to one installed extension schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionConfiguration {
    /// Exact installed extension version.
    pub extension: ExtensionDependency,
    /// Digest of the immutable configuration JSON Schema.
    pub schema_digest: [u8; 32],
    /// Immutable provider-owned JSON configuration.
    pub content: FileRef,
}

/// Immutable agent selection captured for task admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionAdmission {
    source: EventReference,
    selected: Vec<ExtensionDependency>,
    configurations: Vec<ExtensionConfiguration>,
}

impl ExtensionAdmission {
    pub(crate) fn from_owned_value_with_work(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(native_custody::is_native) {
            return native_custody::admission_from_owned_value(value, work);
        }
        let admission: Self = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        admission.validate(work)?;
        Ok((admission, 0))
    }

    /// Actual heap ownership, excluding inline proofs and identifiers.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = (self.source.authority.id.capacity() as u64)
            .checked_add((self.selected.capacity() as u64)
                .checked_mul(std::mem::size_of::<ExtensionDependency>() as u64)
                .ok_or_else(|| Error::Invalid("admission selection ownership overflows".into()))?)
            .and_then(|bytes| bytes.checked_add((self.configurations.capacity() as u64)
                .checked_mul(std::mem::size_of::<ExtensionConfiguration>() as u64)?))
            .ok_or_else(|| Error::Invalid("admission ownership overflows".into()))?;
        for extension in &self.selected {
            bytes = bytes.checked_add(extension.name.capacity() as u64)
                .ok_or_else(|| Error::Invalid("admission ownership overflows".into()))?;
        }
        for configuration in &self.configurations {
            let content = configuration.content.logical_owned_bytes()?;
            bytes = bytes.checked_add(configuration.extension.name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(content))
                .ok_or_else(|| Error::Invalid("admission configuration ownership overflows".into()))?;
        }
        Ok(bytes)
    }

    pub(crate) fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let cloned = self.copy_with_quota(&mut quota)?;
        quota.transfer();
        Ok(cloned)
    }

    pub(crate) fn copy_with_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() { return Ok(self.clone()); }
        let source = native_custody::copy_reference(&self.source, quota)?;
        let selected = native_custody::copy_vector(
            &self.selected, quota, native_custody::copy_dependency)?;
        let configurations = native_custody::copy_vector(
            &self.configurations, quota, native_custody::copy_configuration)?;
        Ok(Self { source, selected, configurations })
    }

    /// Exact committed selection event that supplied these bindings.
    #[must_use]
    pub const fn source(&self) -> &EventReference {
        &self.source
    }

    /// Dependency-ordered installed versions frozen for this admission.
    #[must_use]
    pub fn selected(&self) -> &[ExtensionDependency] {
        &self.selected
    }

    /// File-backed configuration revisions frozen for this admission.
    #[must_use]
    pub fn configurations(&self) -> &[ExtensionConfiguration] {
        &self.configurations
    }

    /// Checks the frozen selection and its ordered configuration records.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        use crate::fork::boundary_error;
        use crate::wire::ErrorCode::{Invalid, Conflict};
        if self.source.authority.kind != AggregateKind::Agent || self.source.revision == 0 {
            return Err(boundary_error(work, Invalid, "extension admission source is invalid"));
        }
        self.source.authority.validate_with_work(work)?;
        for (index, extension) in self.selected.iter().enumerate() {
            if let Some(io) = work.as_deref() { io.check_cancellation()?; }
            if !extension_name_valid(&extension.name) || extension.version == 0 {
                return Err(boundary_error(work, Invalid, "extension dependency is invalid"));
            }
            if self.selected[..index].iter().any(|prior| prior.name == extension.name) {
                return Err(boundary_error(work, Conflict, "duplicate selected extension"));
            }
        }
        let mut last_position = None;
        for configuration in &self.configurations {
            if let Some(io) = work.as_deref() { io.check_cancellation()?; }
            if !extension_name_valid(&configuration.extension.name)
                || configuration.extension.version == 0
                || configuration.schema_digest == [0; 32]
                || configuration.content.descriptor().media_type() != "application/json"
            {
                return Err(boundary_error(work, Invalid, "extension configuration is invalid"));
            }
            configuration.content.validate(work)?;
            let position = self.selected.iter().position(|extension| extension == &configuration.extension);
            if position.is_none_or(|position| last_position.is_some_and(|last| position <= last)) {
                return Err(boundary_error(work, Conflict,
                    "extension admission configuration is not uniquely selected"));
            }
            last_position = position;
        }
        Ok(())
    }

}

impl ExtensionConfiguration {
    /// Validates the envelope independently of the installed schema.
    pub fn validate(&self) -> Result<()> {
        self.extension.validate()?;
        if self.schema_digest == [0; 32]
            || self.content.descriptor().media_type() != "application/json"
        {
            return Err(Error::Invalid("extension configuration is invalid".into()));
        }
        self.content.validate(&mut None)
    }
}

/// Immutable schema and implementation binding for one extension version.
#[derive(Clone, Debug, PartialEq, Serialize)]
struct ExtensionBinding {
    schema: Value,
    schema_digest: [u8; 32],
    implementation_digest: [u8; 32],
    fork_policy: ExtensionForkPolicy,
    dependencies: BTreeSet<ExtensionDependency>,
    configuration_schema: Option<Value>,
    configuration_schema_digest: Option<[u8; 32]>,
}

/// Immutable-version registry for namespaced extension implementations and state schemas.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchemaRegistry {
    schemas: Option<Arc<BTreeMap<(String, u32), ExtensionBinding>>>,
}

impl SchemaRegistry {
    /// Creates an empty registry that rejects every extension event.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schemas: None,
        }
    }

    pub(crate) fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(io) = work.as_deref_mut().filter(|io| native_custody::is_native(&**io)) {
            io.check_cancellation()?;
        }
        Ok(self.clone())
    }

    /// Installs an immutable schema version with dependencies and optional configuration.
    #[allow(clippy::too_many_arguments, reason = "one immutable extension declaration")]
    pub fn register(
        &mut self,
        name: impl Into<String>,
        version: u32,
        schema: Value,
        implementation_digest: [u8; 32],
        fork_policy: ExtensionForkPolicy,
        dependencies: impl IntoIterator<Item = ExtensionDependency>,
        configuration_schema: Option<Value>,
    ) -> Result<()> {
        let name = name.into();
        validate_extension_name(&name)?;
        if version == 0 || implementation_digest == [0; 32] {
            return Err(Error::Invalid(
                "extension binding needs a namespaced name, positive version, and implementation digest".into(),
            ));
        }
        crate::contract::compile_json_schema(&schema, "extension", None)?;
        if let Some(configuration_schema) = &configuration_schema {
            crate::contract::compile_json_schema(configuration_schema, "configuration", None)?;
        }
        let mut required = BTreeSet::new();
        for dependency in dependencies {
            dependency.validate()?;
            if dependency.name == name && dependency.version == version {
                return Err(Error::Conflict("extension depends on itself".into()));
            }
            if !required.insert(dependency) {
                return Err(Error::Conflict("duplicate extension dependency".into()));
            }
        }
        let binding = ExtensionBinding {
            schema_digest: crate::contract::canonical_json_digest(&schema)?,
            schema,
            implementation_digest,
            fork_policy,
            dependencies: required,
            configuration_schema_digest: configuration_schema
                .as_ref()
                .map(crate::contract::canonical_json_digest)
                .transpose()?,
            configuration_schema,
        };
        let key = (name, version);
        if let Some(existing) = self.schemas.as_deref().and_then(|schemas| schemas.get(&key)) {
            return if existing == &binding {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "extension identity is already pinned to another implementation".into(),
                ))
            };
        }
        Arc::make_mut(self.schemas.get_or_insert_with(|| Arc::new(BTreeMap::new()))).insert(key, binding);
        Ok(())
    }

    /// Resolves selected versions in dependency-first order.
    pub fn activation_order(
        &self, selected: &[ExtensionDependency],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Vec<ExtensionDependency>, u64)> {
        let mut owned = crate::fork::CodecQuota::observed(work)?;
        let mut chosen = BTreeMap::<&str, &ExtensionDependency>::new();
        let mut pending = BTreeSet::new();
        let mut graph_bytes = 0_u64;
        for extension in selected {
            if !pending.contains(extension) {
                owned.slots::<&ExtensionDependency>(1)?;
                graph_bytes = graph_bytes.checked_add(std::mem::size_of::<&ExtensionDependency>() as u64)
                    .ok_or_else(|| Error::Invalid("activation graph ownership overflows".into()))?;
                pending.insert(extension);
            }
        }
        while let Some(extension) = pending.pop_last() {
            let bytes = std::mem::size_of::<&ExtensionDependency>() as u64;
            owned.release(bytes)?;
            graph_bytes -= bytes;
            owned.checkpoint()?;
            extension.validate()?;
            if let Some(prior) = chosen.get(extension.name.as_str()) {
                if prior.version != extension.version {
                    return Err(crate::fork::boundary_error(owned.work(), crate::wire::ErrorCode::Conflict,
                        "extension requires conflicting versions"));
                }
                continue;
            }
            let binding = self.pinned_binding(&extension.name, extension.version, None, owned.work())?;
            owned.slots::<(&str, &ExtensionDependency)>(1)?;
            graph_bytes = graph_bytes.checked_add(std::mem::size_of::<(&str, &ExtensionDependency)>() as u64)
                .ok_or_else(|| Error::Invalid("activation graph ownership overflows".into()))?;
            chosen.insert(&extension.name, extension);
            for dependency in &binding.dependencies {
                if !pending.contains(dependency) {
                    owned.slots::<&ExtensionDependency>(1)?;
                    graph_bytes = graph_bytes.checked_add(std::mem::size_of::<&ExtensionDependency>() as u64)
                    .ok_or_else(|| Error::Invalid("activation graph ownership overflows".into()))?;
                    pending.insert(dependency);
                }
            }
        }
        let mut order = native_custody::reserve_copy_vector::<ExtensionDependency>(chosen.len(), &mut owned)?;
        while order.len() < chosen.len() {
            let mut next = None;
            for extension in chosen.values().copied() {
                owned.checkpoint()?;
                if order.iter().any(|ready| ready == extension) { continue; }
                let binding = self.pinned_binding(&extension.name, extension.version, None, owned.work())?;
                let mut ready = true;
                for dependency in &binding.dependencies {
                    owned.checkpoint()?;
                    if !order.contains(dependency) { ready = false; break; }
                }
                if ready { next = Some(extension); break; }
            }
            let next = next.ok_or_else(|| crate::fork::boundary_error(owned.work(),
                crate::wire::ErrorCode::Conflict, "extension dependency cycle"))?;
            order.push(native_custody::copy_dependency(next, &mut owned)?);
        }
        drop((chosen, pending));
        owned.release(graph_bytes)?;
        let produced = owned.retained();
        owned.transfer();
        Ok((order, produced))
    }

    fn pinned_binding(
        &self, name: &str, version: u32,
        expected: Option<([u8; 32], [u8; 32], ExtensionForkPolicy)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&ExtensionBinding> {
        let mut selected = None;
        if let Some(schemas) = &self.schemas {
            for ((candidate, candidate_version), binding) in schemas.iter() {
                if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
                if candidate == name && *candidate_version == version { selected = Some(binding); break; }
            }
        }
        let binding = selected.ok_or_else(|| crate::fork::boundary_error(work,
            crate::wire::ErrorCode::Unsupported, "extension implementation is not installed"))?;
        if expected.is_some_and(|(schema, implementation, policy)|
            schema != binding.schema_digest || implementation != binding.implementation_digest || policy != binding.fork_policy)
        {
            return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Conflict, "extension binding mismatch"));
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
        content.validate(&mut None)?;
        if content.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("extension content must be JSON".into()));
        }
        content.descriptor().verify(bytes, &mut None)?;
        let value: Value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("extension content is not JSON: {error}")))?;
        let binding = self.pinned_binding(name, version, None, &mut None)?;
        crate::contract::validate_json_schema_value(&binding.schema, &value, "extension payload", None)?;
        Ok(())
    }

    fn configuration_record_in_quota(
        &self, extension: &ExtensionDependency, content: &FileRef,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<ExtensionConfiguration> {
        extension.validate()?;
        let binding = self.pinned_binding(&extension.name, extension.version, None, quota.work())?;
        let schema_digest = binding.configuration_schema_digest
            .ok_or_else(|| Error::Invalid("extension does not declare configuration".into()))?;
        let record = ExtensionConfiguration {
            extension: native_custody::copy_dependency(extension, quota)?,
            schema_digest, content: native_custody::copy_file(content, quota)?,
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn implementation_digest(
        &self, name: &str, version: u32,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        Ok(self.pinned_binding(name, version, None, work)?.implementation_digest)
    }

    /// Checks configuration content against the registry and linked implementation.
    pub(crate) fn verify_configuration_binding(
        &self, record: &ExtensionConfiguration, expected_implementation_digest: [u8; 32],
        bytes: &[u8], limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        let _metadata_hold;
        #[cfg(feature = "filesystem")]
        let _metadata;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            _metadata = io.encode(record, io.remaining()?.bytes_encoded, false)?;
            _metadata_hold = io.guard_owned(_metadata.len() as u64)?;
        }
        limits.validate_file(&record.content, &mut None)?;
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| Error::Invalid("configuration byte length is not portable".into()))?;
        if byte_length > limits.file_bytes
            || byte_length > limits.render_bytes
            || record.content.descriptor().byte_length() > limits.render_bytes
        {
            return Err(Error::Invalid(
                "configuration exceeds native option byte limits".into(),
            ));
        }
        record.validate()?;
        let binding =
            crate::executor::with_history_io(io, |work| self.pinned_binding(&record.extension.name, record.extension.version, None, work))?;
        if binding.configuration_schema_digest != Some(record.schema_digest) {
            return Err(Error::Conflict("configuration registry record mismatch".into()));
        }
        if binding.implementation_digest != expected_implementation_digest {
            return Err(Error::Conflict(
                "configuration implementation binding mismatch".into(),
            ));
        }
        self.validate_configuration_bytes(&record.extension, &record.content, bytes, io)
    }

    fn validate_configuration_bytes(
        &self, extension: &ExtensionDependency, content: &FileRef, bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        extension.validate()?;
        crate::executor::with_history_io(io, |work| content.validate(work))?;
        if content.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("extension configuration is invalid".into()));
        }
        #[cfg(feature = "filesystem")]
        let hash_work = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.preflight(hash_work)?; io.charge(hash_work)?; }
        content.descriptor().verify(bytes, &mut None)?;
        let value;
        #[cfg(feature = "filesystem")]
        let decoded;
        let value = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => {
                decoded = io.decode(std::marker::PhantomData::<Value>, bytes, crate::wire::ErrorCode::Storage)?;
                &decoded.0
            }
            _ => {
                value = crate::contract::json_from_slice(bytes).map_err(|error| {
                    Error::Invalid(format!("extension configuration is not JSON: {error}"))
                })?;
                &value
            }
        };
        let binding = crate::executor::with_history_io(io, |work| self.pinned_binding(&extension.name, extension.version, None, work))?;
        let schema = binding
            .configuration_schema
            .as_ref()
            .ok_or_else(|| Error::Invalid("extension does not declare configuration".into()))?;
        crate::contract::validate_json_schema_value(schema, value, "configuration", io)
    }
}

/// Restorable deterministic reducer state for one authority.
#[derive(Debug, PartialEq)]
pub struct Reducer {
    authority: Authority,
    authority_verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    revision: u64,
    lifecycle: LifecycleState,
    active_extensions: Vec<ExtensionDependency>,
    extension_states: BTreeMap<String, (EventReference, ExtensionRecord)>,
    configured_extensions: BTreeMap<ExtensionDependency, ExtensionConfiguration>,
    active_configurations: Vec<ExtensionConfiguration>,
    events: VecDeque<Event>,
    operation_positions: BTreeMap<OperationId, u64>,
    resident_event_limit: usize,
    active_extension_revision: Option<u64>,
    effects: BTreeMap<EffectId, EffectState>,
    indexed_terminal_effects: VecDeque<EffectId>,
    effects_archived: bool,
    resident_terminal_effect_limit: usize,
    forks: BTreeMap<Authority, ForkSeed>,
    published_merges: BTreeSet<(String, Vec<u8>)>,
    conversation: ConversationState,
    latest_context_checkpoint: Option<FileRef>,
    interactions: BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
    native_custody: NativeComponentCustody,
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
            active_extensions: Vec::new(),
            extension_states: BTreeMap::new(),
            configured_extensions: BTreeMap::new(),
            active_configurations: Vec::new(),
            events: VecDeque::new(),
            operation_positions: BTreeMap::new(),
            resident_event_limit: usize::MAX,
            active_extension_revision: None,
            effects: BTreeMap::new(),
            indexed_terminal_effects: VecDeque::new(),
            effects_archived: false,
            resident_terminal_effect_limit: usize::MAX,
            forks: BTreeMap::new(),
            published_merges: BTreeSet::new(),
            conversation: ConversationState::default(),
            latest_context_checkpoint: None,
            interactions: BTreeMap::new(),
            native_custody: NativeComponentCustody::default(),
        }
    }

    /// Moves only already-paid native constructor fields into intrinsic custody.
    pub(crate) fn new_with_preclaimed_work(
        authority: Authority, authority_verifier: AuthorityVerifier, schemas: SchemaRegistry,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            let mut native_custody = NativeComponentCustody::default();
            let inputs = (authority, authority_verifier, schemas);
            let installed: Result<()> = (|| {
                let authority = io.take_native_custody(inputs.0.id.capacity() as u64)?;
                let _retired = native_custody.install(Some((NativeComponentKey::Authority, authority, None)), &mut Some(&mut *io))?;
                let base_bytes = inputs.1.logical_owned_bytes()?;
                let base = io.take_native_custody(base_bytes)?;
                let _retired = native_custody.install(Some((NativeComponentKey::Base, base, None)), &mut Some(&mut *io))?;
                Ok(())
            })();
            if let Err(error) = installed {
                drop(inputs);
                return Err(error);
            }
            let mut reducer = Self::new(inputs.0, inputs.1, inputs.2);
            reducer.native_custody = native_custody;
            return Ok(reducer);
        }
        let _ = work;
        Ok(Self::new(authority, authority_verifier, schemas))
    }

    /// Deep-copies this projection only after funding every new live component.
    pub(crate) fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.original_work().is_some()
            && !work.as_deref().is_some_and(|io| io.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native reducer cloning requires explicit native work",
            });
        }
        native_custody::copy_reducer(self, work)
    }

    fn replace_extension_state_with_work(
        &mut self, record: &ExtensionRecord, revision: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let (entry, _retired) = self.native_custody.produce(
            &NativeComponentKey::ExtensionState(Cow::Borrowed(&record.name)), work,
            |quota| {
                native_custody::retain_entry::<(String, (EventReference, ExtensionRecord))>(quota)?;
                let key = native_custody::copy_text(&record.name, quota)?;
                let authority = native_custody::copy_authority(&self.authority, quota)?;
                let record = native_custody::copy_extension_record(record, quota)?;
                Ok((key, (EventReference { authority, revision }, record)))
            },
        )?;
        if work.as_deref().is_some_and(native_custody::is_native) {
            drop(self.extension_states.remove_entry(&record.name));
        }
        self.extension_states.insert(entry.0, entry.1);
        Ok(())
    }

    fn reserve_deque_buffer<T>(
        custody: &mut NativeComponentCustody, key: NativeComponentKey<'_>,
        payload: &mut VecDeque<T>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if !work.as_deref().is_some_and(native_custody::is_native)
            || payload.len() < payload.capacity()
        { return Ok(()); }
        let capacity = payload.capacity().max(1).checked_mul(2)
            .ok_or_else(|| Error::Invalid("component buffer capacity overflows".into()))?;
        let copied = payload.len().checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::Invalid("component buffer move size overflows".into()))? as u64;
        work.as_deref().expect("native buffer observer").admit_copy(copied)?;
        let (replacement, retired) = custody.produce(&key, work,
            |quota| native_custody::reserve_copy_vector::<T>(capacity, quota))?;
        let mut replacement = VecDeque::from(replacement);
        if let Err(error) = work.as_deref_mut().expect("native buffer observer").copy_bytes(copied) {
            drop(replacement);
            custody.rollback(&key, retired);
            return Err(error);
        }
        replacement.append(payload);
        let old_payload = std::mem::replace(payload, replacement);
        drop(old_payload);
        drop(retired);
        Ok(())
    }

    fn append_conversation_message_with_work(
        &mut self, message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.conversation.validate_append(message, work)?;
        let (message, retired) = self.native_custody.produce(
            &NativeComponentKey::ConversationMessage(message.sequence), work,
            |quota| native_custody::copy_message(message, quota),
        )?;
        let sequence = message.sequence;
        if let Err(error) = self.conversation.append(message, work) {
            drop(self.native_custody.rollback(&NativeComponentKey::ConversationMessage(sequence), retired));
            return Err(error);
        }
        drop(retired);
        Ok(())
    }

    fn merge_key_with_work(
        receipt: &ProjectMergeReceipt,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<((String, Vec<u8>), u64)> {
        if !work.as_deref().is_some_and(native_custody::is_native) {
            return Ok(((receipt.target_project.storage_name(work)?,
                receipt.provider_operation_id.clone()), 0));
        }
        let digest = crate::contract::canonical_json_digest_with_work(
            &receipt.target_project, acyclic_stream::MAX_RECORD_BYTES as u64, work,
        )?;
        let hex = blake3::Hash::from_bytes(digest).to_hex();
        let mut name = String::new();
        let mut operation = Vec::new();
        let io = work.as_deref_mut().expect("native merge observer");
        io.reserve_text(&mut name, "harness-".len() + hex.len())?;
        io.copy_bytes(("harness-".len() + hex.len()) as u64)?;
        name.push_str("harness-");
        name.push_str(hex.as_str());
        io.reserve_bytes(&mut operation, receipt.provider_operation_id.len())?;
        io.copy_bytes(receipt.provider_operation_id.len() as u64)?;
        operation.extend_from_slice(&receipt.provider_operation_id);
        let owned = (name.capacity() as u64).checked_add(operation.capacity() as u64)
            .ok_or_else(|| Error::Invalid("merge key custody overflows".into()))?;
        Ok(((name, operation), owned))
    }

    /// Returns this reducer's authority.
    #[must_use]
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    pub(crate) fn event_verifier(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        self.authority_verifier.copy(work)
    }

    /// Returns the current gapless revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Captures the original authenticated selection and its registry atomically.
    pub(crate) fn original_extension_binding(&self) -> Result<(Option<ExtensionAdmission>, SchemaRegistry)> {
        Ok((self.extension_admission()?, self.schemas.clone()))
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
    /// Exact installed implementation that must execute a state migration into this extension version.
    pub fn extension_implementation_digest(&self, name: &str, version: u32) -> Result<[u8; 32]> {
        Ok(self
            .schemas
            .pinned_binding(name, version, None, &mut None)?
            .implementation_digest)
    }
    /// Authenticates and checks the exact migration boundary before a host reads old state or stages new bytes.
    pub(crate) fn preflight_extension_migration(
        &self,
        scope: &Scope,
        expected_revision: u64,
        previous: &EventReference,
        name: &str,
        to_version: u32,
        destination: &VolumeRef,
    ) -> Result<()> {
        self.authority_verifier.verify_audience(&self.authority, &mut None)?;
        self.authority_verifier.verify(scope, &mut None)?;
        require_capability(scope, capability::EXTENSION_MIGRATE)?;
        ContentGrant::authorize_volume(
            &self.authority_verifier,
            scope,
            destination,
            VolumeOperation::Write,
            acyclic_stream::MAX_RECORD_BYTES as u64, &mut None,
        )?;
        if expected_revision != self.revision {
            return Err(Error::Conflict(
                "extension migration revision changed".into(),
            ));
        }
        let (source, current) = self
            .extension_states
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
        if source != previous
            || previous.authority != self.authority
            || current.version == to_version
        {
            return Err(Error::Conflict(
                "extension migration source or target is not exact".into(),
            ));
        }
        self.schemas.pinned_binding(name, to_version, None, &mut None)?;
        Ok(())
    }

    /// Validates staged configuration bytes before an event can be admitted.
    pub fn validate_configuration_bytes(
        &self, extension: &ExtensionDependency, content: &FileRef, bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.schemas.validate_configuration_bytes(extension, content, bytes, io)
    }

    /// Returns the fixed aggregate lifecycle projection.
    #[must_use]
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Dependency-first extension versions selected for new agent admissions.
    #[must_use]
    pub fn active_extensions(&self) -> &[ExtensionDependency] {
        &self.active_extensions
    }

    /// Latest retained state event for one namespace.
    #[must_use]
    pub fn extension_state(&self, name: &str) -> Option<&(EventReference, ExtensionRecord)> {
        self.extension_states.get(name)
    }

    /// Exact configuration revisions pinned by the most recent activation.
    #[must_use]
    pub fn active_configurations(&self) -> &[ExtensionConfiguration] {
        &self.active_configurations
    }

    /// Captures the last committed activation for immutable task admission.
    pub fn extension_admission(&self) -> Result<Option<ExtensionAdmission>> {
        if self.authority.kind != AggregateKind::Agent {
            return Err(Error::Invalid(
                "extension admission requires an agent aggregate".into(),
            ));
        }
        let Some(revision) = self.active_extension_revision else {
            return Ok(None);
        };
        let admission = ExtensionAdmission {
            source: EventReference {
                authority: self.authority.clone(),
                revision,
            },
            selected: self.active_extensions.clone(),
            configurations: self.active_configurations.clone(),
        };
        admission.validate(&mut None)?;
        Ok(Some(admission))
    }

    fn selected_configurations_in_quota(
        &self, selected: &[ExtensionDependency],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Vec<ExtensionConfiguration>> {
        let mut count = 0;
        for extension in selected {
            if self.schemas.pinned_binding(&extension.name, extension.version, None, quota.work())?
                .configuration_schema.is_some() { count += 1; }
        }
        let mut configurations = native_custody::reserve_copy_vector(count, quota)?;
        for extension in selected {
            let binding = self.schemas.pinned_binding(&extension.name, extension.version, None, quota.work())?;
            if binding.configuration_schema.is_some() {
                let record = self.configured_extensions.get(extension)
                    .ok_or_else(|| Error::Conflict(format!("extension {}@{} is not configured", extension.name, extension.version)))?;
                configurations.push(native_custody::copy_configuration(record, quota)?);
            }
        }
        Ok(configurations)
    }
    fn selected_configuration_matches_with_work(
        &self, selected: &[ExtensionDependency], configurations: &[ExtensionConfiguration],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (order, produced) = self.schemas.activation_order(selected, quota.work())?;
        quota.adopt(produced)?;
        if order.as_slice() != selected { return Ok(false); }
        let mut configurations = configurations.iter();
        for extension in selected {
            let binding = self.schemas.pinned_binding(&extension.name, extension.version, None, quota.work())?;
            if binding.configuration_schema.is_some() {
                let expected = self.configured_extensions.get(extension)
                    .ok_or_else(|| Error::Conflict(format!("extension {}@{} is not configured", extension.name, extension.version)))?;
                if configurations.next() != Some(expected) { return Ok(false); }
            }
        }
        Ok(configurations.next().is_none())
    }

    /// Returns the built-in conversation projection for conversation aggregates.
    #[must_use]
    pub fn conversation(&self) -> Option<&ConversationState> {
        (self.authority.kind == AggregateKind::Conversation).then_some(&self.conversation)
    }

    /// Enumerates context selections in the resident event suffix, in admission order.
    pub fn resident_context_selections(&self) -> impl Iterator<Item = &ModelContextSelection> {
        self.events.iter().filter_map(|event| match &event.payload {
            EventPayload::ModelContextSelected { selection } => Some(selection),
            _ => None,
        })
    }

    /// Latest owner-published canonical base, maintained by selection replay.
    #[must_use]
    pub fn latest_context_checkpoint(&self) -> Option<&FileRef> {
        self.latest_context_checkpoint.as_ref()
    }

    /// Returns the exact selection committed for one operation, even after later conversation messages changed the aggregate's current tail.
    #[must_use]
    pub fn context_selection_for_operation(
        &self,
        operation_id: OperationId,
    ) -> Option<&ModelContextSelection> {
        self.event_for_operation(operation_id)
            .and_then(|event| match &event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            })
    }

    pub(crate) fn verify_command_scope_with_work(
        &self, command: &Command,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.authority_verifier.verify_audience(&self.authority, work)?;
        let key = match work.as_deref_mut() {
            Some(work) => work.clone_text(&command.idempotency_key.0)?,
            None => command.idempotency_key.0.clone(),
        };
        let capacity = key.capacity() as u64;
        let valid = IdempotencyKey::new(key).map(|_| ());
        if let Some(work) = work.as_deref_mut() { work.release_owned(capacity)?; }
        valid?;
        self.authority_verifier.verify(&command.scope, work)
    }

    pub(crate) fn verify_retained_command_with_work(
        &self, command: &Command, event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.verify_command_scope_with_work(command, work)?;
        self.authority_verifier.verify_event(event, work)?;
        if event.operation_id != command.operation_id
            || event.intent_digest != crate::contract::canonical_json_digest_with_work(command, crate::store::default_projection_read_limits().maximum_bytes, work)?
        {
            return Err(Error::Conflict(
                "operation identity is already bound to another intent".into(),
            ));
        }
        Ok(())
    }

    /// Exact aggregate revision committed for a stable operation identity.
    #[must_use]
    pub fn operation_revision(&self, operation_id: OperationId) -> Option<u64> {
        self.event_for_operation(operation_id)
            .map(|event| event.revision)
    }

    fn event_for_operation(&self, operation_id: OperationId) -> Option<&Event> {
        self.operation_positions
            .get(&operation_id)
            .and_then(|revision| {
                let first = self.events.front()?.revision;
                let position = usize::try_from(revision.checked_sub(first)?).ok()?;
                self.events.get(position)
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
    #[allow(clippy::needless_pass_by_value, reason = "command ownership ends at application")]
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.apply",
            level = "debug",
            skip_all,
            fields(rev = self.revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub fn apply(&mut self, command: Command) -> Result<ApplyResult> {
        crate::obs::outcome(match self.plan(&command) {
            Ok(ApplyResult::Applied { event }) => self
                .apply_committed(event.clone(), &mut None)
                .map(|_| ApplyResult::Applied { event }),
            planned => planned,
        })
    }

    /// Plans a command without advancing authoritative state.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.plan",
            level = "debug",
            skip_all,
            fields(rev = self.revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub fn plan(&self, command: &Command) -> Result<ApplyResult> {
        crate::obs::outcome(self.plan_with_migration_boundary_with_work(
            command,
            false,
            false,
            None, &mut None).map(|(result, _)| result))
    }

    /// Returns the original planner's already-retained output allowance.
    pub(crate) fn plan_indexed_with_retained_quota(
        &self, command: &Command, migration_verified: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if migration_verified && !matches!(&command.action, Action::MigrateExtensionState { .. }) {
            return crate::obs::outcome(Err(Error::Invalid(
                "verified migration planner requires a migration action".into(),
            )));
        }
        crate::obs::outcome(self.plan_with_migration_boundary_with_work(
            command,
            migration_verified,
            true,
            None,
            work,
        ))
    }

    /// Selects only fresh effect transitions whose original projection may have been retired.
    pub(crate) fn archived_effect_for_command(&self, command: &Command) -> Option<EffectId> {
        let effect_id = match &command.action {
            Action::MarkEffectDispatched { effect_id, .. } => *effect_id,
            Action::ResolveEffect { observation } => observation.effect_id,
            _ => return None,
        };
        (self.effects_archived && !self.effects.contains_key(&effect_id)).then_some(effect_id)
    }

    pub(crate) fn plan_with_archived_effect(
        &self, command: &Command, effect_id: EffectId, effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if self.archived_effect_for_command(command) != Some(effect_id)
            || !settled_effect(&effect.status)
        {
            return Err(Error::Storage(
                "archived effect does not match terminal planning target".into(),
            ));
        }
        crate::obs::outcome(self.plan_with_migration_boundary_with_work(
            command, false, true, Some((effect_id, effect)), work,
        ))
    }

    pub(crate) fn set_resident_event_limit(&mut self, maximum: usize) -> Result<()> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "resident event allowance must be positive".into(),
            ));
        }
        self.resident_event_limit = maximum;
        self.trim_resident_events();
        Ok(())
    }

    fn trim_resident_events(&mut self) {
        while self.events.len() > self.resident_event_limit {
            if let Some(event) = self.events.pop_front() {
                let revision = event.revision;
                let operation = event.operation_id;
                self.operation_positions.remove(&operation);
                drop(event);
                self.native_custody.remove(&NativeComponentKey::Event(revision));
                self.native_custody.remove(&NativeComponentKey::OperationPosition(operation));
            }
        }
    }

    /// Last revision no longer available through the synchronous resident event reader.
    #[must_use]
    pub fn archived_through_revision(&self) -> u64 {
        self.events
            .front()
            .map_or(self.revision, |event| event.revision - 1)
    }

    fn plan_with_migration_boundary_with_work(
        &self,
        command: &Command,
        migration_verified: bool,
        identity_checked: bool,
        archived_effect: Option<(EffectId, &EffectState)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.original_work().is_some()
            && !work.as_deref().is_some_and(native_custody::is_native)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native reducer planning requires explicit native work",
            });
        }
        self.verify_command_scope_with_work(command, work)?;
        let intent = crate::contract::canonical_json_digest_with_work(command, crate::store::default_projection_read_limits().maximum_bytes, work)?;
        let mut retained_quota = 0;
        if let Some(event) = self.event_for_operation(command.operation_id) {
            if event.intent_digest == intent {
                if work.as_deref().is_some_and(native_custody::is_native) {
                    let (event, produced) = native_event_clone_with_custody(event, work)?;
                    return Ok((ApplyResult::Replayed { event }, produced));
                }
                if let Some(observer) = work.as_deref_mut() {
                    let reservation = snapshot::source_inventory(event)?;
                    if matches!(&event.payload, EventPayload::ForkPublished { .. }) {
                        observer.admit_allocation(std::mem::size_of::<ForkSeed>() as u64)?;
                    }
                    observer.admit_owned(reservation)?;
                    observer.retain_owned(reservation)?;
                    retained_quota = reservation;
                }
                return Ok((ApplyResult::Replayed { event: event.clone() }, retained_quota));
            }
            return Err(Error::Conflict(
                "operation identity is already bound to another intent".into(),
            ));
        }
        if !identity_checked && (self.archived_through_revision() != 0 || self.effects_archived) {
            return Err(Error::Unsupported(
                "operation identity requires authoritative archive lookup".into(),
            ));
        }
        if command.expected_revision != self.revision {
            return Err(Error::Conflict(format!(
                "expected revision {}, found {}",
                command.expected_revision, self.revision
            )));
        }
        self.authorize_with_work(
            |capability| require_capability(&command.scope, capability),
            command.scope.agent(),
            command.operation_id,
            command.action.kind(),
            archived_effect,
            work,
        )?;
        match &command.action {
            Action::MigrateExtensionState { content, .. } => {
                ContentGrant::authorize_volume(
                    &self.authority_verifier, &command.scope, content.volume(), VolumeOperation::Write,
                    acyclic_stream::MAX_RECORD_BYTES as u64, work,
                )?;
                if !migration_verified {
                    return Err(Error::Unsupported(
                        "extension migration requires an executing content-admission host".into(),
                    ));
                }
            }
            Action::BindConversation { agent }
                if command.scope.agent().is_some_and(|acting| acting != *agent) =>
            {
                return Err(Error::Unauthorized(
                    "conversation binding does not match the authenticated agent".into(),
                ));
            }
            Action::PublishFork { seed } => self.require_fresh_fork_with_work(seed, work)?,
            _ => {}
        }
        validate_causal_parent_with_work(
            &self.authority,
            self.revision,
            command.causal_parent.as_ref(), work,
        )?;
        let revision = next_revision(self.revision)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let payload = self.transition_with_work(&command.action, archived_effect, &mut quota)?;
        let mut event = Event {
            revision, operation_id: command.operation_id, intent_digest: intent,
            scope: RecordedScope {
                id: native_custody::copy_text(&command.scope.id, &mut quota)?,
                issuer: native_custody::copy_text(&command.scope.issuer, &mut quota)?,
                capabilities: native_custody::copy_capabilities(&command.scope.capabilities, &mut quota)?,
                agent: command.scope.agent,
            },
            attestation: [0; 32],
            causal_parent: command.causal_parent.as_ref().map(|parent|
                native_custody::copy_reference(parent, &mut quota)).transpose()?,
            payload,
        };
        event.attestation = self.authority_verifier.attest_event(&event, quota.work())?;
        retained_quota = quota.retained();
        quota.transfer();
        Ok((ApplyResult::Applied { event }, retained_quota))
    }

    fn transition_effect<'a>(
        &'a self,
        effect_id: &EffectId,
        archived: Option<(EffectId, &'a EffectState)>,
    ) -> Result<&'a EffectState> {
        self.effects
            .get(effect_id)
            .or_else(|| {
                archived
                    .filter(|(id, _)| id == effect_id)
                    .map(|(_, state)| state)
            })
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))
    }

    /// Authorizes one transition identically when planned and when committed.
    fn authorize_with_work(
        &self,
        require: impl Fn(&str) -> Result<()>,
        agent: Option<AgentId>,
        operation_id: OperationId,
        (kind, subject): (TransitionKind, Subject<'_>),
        archived_effect: Option<(EffectId, &EffectState)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        require(kind.spec().2)?;
        match subject {
            Subject::None => Ok(()),
            Subject::PlannedEffect => require(capability::EFFECT_PLAN),
            Subject::Effect(effect_id) => {
                let effect = self.transition_effect(effect_id, archived_effect)?;
                require(&capability::effect_provider(&effect.provider))
            }
            Subject::Interaction(resolution) => {
                validate_interaction_operation(resolution, operation_id)?;
                let (ticket, _) = self
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                require(&ticket.responder_grant())
            }
            Subject::Fork(seed) => {
                if self.conversation.agent.is_none() || agent != self.conversation.agent {
                    return Err(Error::Unauthorized(
                        "fork must be published by the parent agent".into(),
                    ));
                }
                let validation = seed.validate(work);
                if work.is_some() {
                    validation?;
                } else if validation.is_err() {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                if seed.operation_id != operation_id || seed.parent != self.authority
                    || seed.parent_revision != self.revision
                {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                let ownership = self.validate_fork_reference_ownership_with_work(seed, work);
                if work.is_some() {
                    ownership?;
                } else if ownership.is_err() {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                let mut owned = crate::fork::CodecQuota::observed(work)?;
                for resource in &seed.resources {
                    if let crate::fork::ResourceRevision::Project { volume, .. } = &resource.source {
                        let capability = owned.volume_capability(volume)?;
                        require(&capability)?;
                    }
                    if let crate::fork::ResourceRevision::PrivateVolume { volume, .. } =
                        &resource.source
                    {
                        if volume.owner()
                            != &VolumeOwner::Agent(agent.ok_or_else(|| {
                                Error::Unauthorized("scratch fork has no parent agent".into())
                            })?)
                        {
                            return Err(Error::Unauthorized(
                                "scratch is not owned by the direct parent".into(),
                            ));
                        }
                        let capability = owned.volume_capability(volume)?;
                        require(&capability)?;
                    }
                }
                Ok(())
            }
            Subject::Merge(receipt) => {
                if receipt.operation_id != operation_id || agent != self.conversation.agent {
                    return Err(Error::Unauthorized(
                        "merge receipt is not bound to the parent agent and operation".into(),
                    ));
                }
                receipt.target_project.require_writer(agent, &None)?;
                Self::require_volume_operation_with_work(&require, &receipt.target_project, VolumeOperation::Write, work)?;
                Self::require_volume_operation_with_work(&require, &receipt.source_project, VolumeOperation::Read, work)?;
                Ok(())
            }
        }
    }
    fn require_volume_operation_with_work(
        require: &impl Fn(&str) -> Result<()>, volume: &VolumeRef, operation: VolumeOperation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(native_custody::is_native) {
            let digest = crate::contract::canonical_json_digest_with_work(
                volume, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
            let hex = blake3::Hash::from_bytes(digest).to_hex();
            let prefix = match operation { VolumeOperation::Read => "volume:read:", VolumeOperation::Write => "volume:write:" };
            let mut capability = [0_u8; 77];
            capability[..prefix.len()].copy_from_slice(prefix.as_bytes());
            capability[prefix.len()..prefix.len()+64].copy_from_slice(hex.as_bytes());
            return require(std::str::from_utf8(&capability[..prefix.len()+64]).expect("hex capability"));
        }
        let _ = work;
        require(&volume.capability(operation, work)?)
    }

    /// Applies an event only after its canonical Stream append commits.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.apply_committed",
            level = "trace",
            skip_all,
            fields(rev = event.revision)
        )
    )]
    pub fn apply_committed(
        &mut self, event: Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ApplyResult> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.original_work().is_some()
            && !work.as_deref().is_some_and(|io| io.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native reducer mutation requires explicit native work",
            });
        }
        self.authority_verifier.verify_audience(&self.authority, work)?;
        if let Some(existing) = self.event_for_operation(event.operation_id) {
            if existing == &event {
                if work.as_deref().is_some_and(native_custody::is_native) {
                    let (event, _) = native_event_clone_with_custody(existing, work)?;
                    return Ok(ApplyResult::Replayed { event });
                }
                if let Some(observer) = work.as_deref_mut() {
                    let reservation = snapshot::source_inventory(existing)?;
                    if matches!(&existing.payload, EventPayload::ForkPublished { .. }) {
                        observer.admit_allocation(std::mem::size_of::<ForkSeed>() as u64)?;
                    }
                    observer.admit_owned(reservation)?;
                    observer.retain_owned(reservation)?;
                }
                return Ok(ApplyResult::Replayed { event: existing.clone() });
            }
            return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Conflict,
                "operation identity is already bound to another event"));
        }
        let expected = next_revision(self.revision)?;
        if event.revision != expected {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(native_custody::is_native) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict, reason: "committed revision does not match the exact successor",
                });
            }
            return Err(Error::Conflict(format!(
                "expected committed revision {expected}, found {}",
                event.revision
            )));
        }
        self.authority_verifier.verify_event(&event, work)?;
        self.authorize_with_work(
            |capability| require_recorded_capability(&event.scope, capability),
            event.scope.agent(),
            event.operation_id,
            event.payload.kind(),
            None,
            work,
        )?;
        validate_causal_parent_with_work(&self.authority, self.revision, event.causal_parent.as_ref(), work)?;
        if let Some(observer) = work.as_deref_mut().filter(|io| !native_custody::is_native(*io)) {
            let reservation = snapshot::source_inventory(&event)?
                .checked_add(std::mem::size_of::<(OperationId, u64)>() as u64)
                .ok_or_else(|| Error::Invalid("committed event clone quota overflows".into()))?;
            if matches!(&event.payload, EventPayload::ForkPublished { .. }) {
                observer.admit_allocation(std::mem::size_of::<ForkSeed>() as u64)?;
            }
            observer.admit_owned(reservation)?;
            observer.retain_owned(reservation)?;
        }
        let retired_position = self.native_custody.install(NativeComponentCustody::prepare(
            &NativeComponentKey::OperationPosition(event.operation_id),
            std::mem::size_of::<(OperationId, u64)>() as u64, work,
        )?, work)?;
        let copied = self.native_custody.produce(
            &NativeComponentKey::Event(event.revision), work,
            |quota| native_custody::copy_event(&event, quota),
        );
        let (cached, retired_event) = match copied {
            Ok(copied) => copied,
            Err(error) => {
                drop(self.native_custody.rollback(&NativeComponentKey::OperationPosition(event.operation_id), retired_position));
                return Err(error);
            }
        };
        let applied = Self::reserve_deque_buffer(&mut self.native_custody,
            NativeComponentKey::EventBuffer, &mut self.events, work)
            .and_then(|()| self.apply_payload_with_work(&event.payload, event.revision, work));
        if let Err(error) = applied {
            drop(cached);
            drop(self.native_custody.rollback(&NativeComponentKey::Event(event.revision), retired_event));
            drop(self.native_custody.rollback(&NativeComponentKey::OperationPosition(event.operation_id), retired_position));
            return Err(error);
        }
        self.revision = event.revision;
        self.operation_positions
            .insert(event.operation_id, event.revision);
        self.events.push_back(cached);
        drop(retired_event);
        drop(retired_position);
        self.trim_resident_events();
        Ok(ApplyResult::Applied { event })
    }

    /// Returns a bounded page strictly after `revision`.
    pub fn events_after(&self, revision: u64, limit: usize) -> Result<Vec<Event>> {
        if revision > self.revision {
            return Err(Error::Invalid("cursor is beyond the aggregate head".into()));
        }
        if revision < self.archived_through_revision() {
            return Err(Error::Unsupported(
                "cursor requires authoritative archive reader".into(),
            ));
        }
        Ok(self
            .events
            .iter()
            .filter(|event| event.revision > revision)
            .take(limit)
            .cloned()
            .collect())
    }

    pub(crate) fn set_resident_terminal_effect_limit(&mut self, maximum: usize) -> Result<()> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "terminal effect cache limit must be positive".into(),
            ));
        }
        if maximum == usize::MAX {
            self.resident_terminal_effect_limit = maximum;
            return Ok(());
        }
        let protected = self
            .effects
            .iter()
            .filter(|(id, effect)| settled_effect(&effect.status) && !self.indexed_terminal_effects.contains(id))
            .count();
        if protected > maximum {
            return Err(Error::Unsupported(
                "unindexed terminal effects exceed cache allowance".into(),
            ));
        }
        self.resident_terminal_effect_limit = maximum;
        self.trim_terminal_effects(protected);
        Ok(())
    }

    pub(crate) fn mark_indexed_terminal_effect_with_work(
        &mut self, event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.original_work().is_some()
            && !work.as_deref().is_some_and(native_custody::is_native)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native terminal indexing requires explicit native work",
            });
        }
        let EventPayload::EffectResolved { observation } = &event.payload else {
            return Ok(());
        };
        if !settled_effect(&observation.status) {
            return Ok(());
        }
        let state = self
            .effects
            .get(&observation.effect_id)
            .ok_or_else(|| Error::Storage("indexed terminal effect is absent".into()))?;
        if state.status != observation.status {
            return Err(Error::Storage(
                "indexed terminal effect differs from admitted result".into(),
            ));
        }
        if self.indexed_terminal_effects.back() != Some(&observation.effect_id) {
            Self::reserve_deque_buffer(&mut self.native_custody,
                NativeComponentKey::TerminalEffectBuffer, &mut self.indexed_terminal_effects, work)?;
            self.indexed_terminal_effects
                .push_back(observation.effect_id);
        }
        if self.resident_terminal_effect_limit == usize::MAX {
            return Ok(());
        }
        self.set_resident_terminal_effect_limit(self.resident_terminal_effect_limit)
    }

    pub(crate) fn enforce_terminal_effect_limit(&mut self) -> Result<()> {
        if self.resident_terminal_effect_limit == usize::MAX {
            return Ok(());
        }
        self.set_resident_terminal_effect_limit(self.resident_terminal_effect_limit)
    }

    fn trim_terminal_effects(&mut self, protected: usize) {
        let budget = self
            .resident_terminal_effect_limit
            .saturating_sub(protected);
        while self.indexed_terminal_effects.len() > budget {
            if let Some(effect) = self.indexed_terminal_effects.pop_front() {
                self.effects.remove(&effect);
                self.native_custody.remove(&NativeComponentKey::EffectAction(effect));
                self.native_custody.remove(&NativeComponentKey::EffectStatus(effect));
                self.native_custody.remove(&NativeComponentKey::EffectAttempts(effect));
                self.effects_archived = true;
            }
        }
    }

    /// Number of resident active and cached terminal effect projections.
    #[must_use]
    pub fn resident_effect_count(&self) -> usize {
        self.effects.len()
    }

    /// Cached known successes/failures; indeterminate effects remain active.
    #[must_use]
    pub fn resident_terminal_effect_count(&self) -> usize {
        self.effects
            .values()
            .filter(|effect| settled_effect(&effect.status))
            .count()
    }

    /// Reconstructs one read-only effect using the same authorization and payload reducer as canonical replay.
    pub(crate) fn project_effect_with_retained_quota(
        authority: Authority, verifier: AuthorityVerifier, effect_id: EffectId,
        events: &[Event], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(EffectState, u64)> {
        let metadata_owned = if work.is_some() {
            (authority.id.capacity() as u64).checked_add(verifier.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("effect metadata custody overflows".into()))?
        } else { 0 };
        let mut retained = 0_u64;
        let mut projection = Self::new(authority, verifier, SchemaRegistry::default());
        let mut previous = 0;
        let result = (|| {
        for event in events {
            if event.revision <= previous || event_effect_id(&event.payload) != Some(effect_id) {
                return Err(Error::Storage(
                    "effect projection has invalid identity or order".into(),
                ));
            }
            projection.authority_verifier.verify_event(event, work)?;
            projection.authorize_with_work(
                |capability| require_recorded_capability(&event.scope, capability),
                event.scope.agent(),
                event.operation_id,
                event.payload.kind(),
                None,
                work,
            )?;
            validate_causal_parent_with_work(
                &projection.authority,
                event.revision - 1,
                event.causal_parent.as_ref(), work,
            )?;
            if let Some(work) = work.as_deref_mut().filter(|io| !native_custody::is_native(&**io)) {
                let owned = snapshot::source_inventory(event)?;
                let next_retained = retained.checked_add(owned)
                    .ok_or_else(|| Error::Invalid("effect projected custody overflows".into()))?;
                work.admit_owned(owned)?;
                work.retain_owned(owned)?;
                retained = next_retained;
            }
            projection.apply_payload_with_work(&event.payload, event.revision, work)?;
            previous = event.revision;
        }
        let effect = projection.effects.remove(&effect_id)
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            retained = projection.native_custody.transfer_effect(effect_id, io)?;
        }
        Ok(effect)
        })();
        drop(projection);
        let released_metadata = match work.as_deref_mut() {
            Some(work) => work.release_owned(metadata_owned),
            None => Ok(()),
        };
        match result {
            Err(error) => {
                if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(retained); }
                Err(error)
            }
            Ok(effect) => match released_metadata {
                Ok(()) => Ok((effect, retained)),
                Err(error) => {
                    drop(effect);
                    if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(retained); }
                    Err(error)
                }
            },
        }
    }

    /// Returns the current resident effect status.
    #[must_use]
    pub fn effect(&self, effect_id: EffectId) -> Option<&EffectState> {
        self.effects.get(&effect_id)
    }

    /// Returns a child only after its publication event committed.
    #[must_use]
    pub fn fork(&self, child: &Authority) -> Option<&ForkSeed> {
        self.forks.get(child)
    }

    fn require_fresh_fork_with_work(
        &self, seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut owned = crate::fork::CodecQuota::observed(work)?;
        let mut child_project = None;
        for resource in &seed.resources {
            owned.checkpoint()?;
            if let crate::fork::ResourceRevision::Project { volume, .. } = &resource.revision {
                child_project = Some(volume);
                break;
            }
        }
        owned.checkpoint()?;
        if seed.child == self.authority {
            return Err(Error::Conflict("invalid fork publication".into()));
        }
        owned.checkpoint()?;
        if self.forks.contains_key(&seed.child) {
            return Err(Error::Conflict("invalid fork publication".into()));
        }
        for prior in self.forks.values() {
            owned.checkpoint()?;
            if prior.child_agent == seed.child_agent {
                return Err(Error::Conflict("invalid fork publication".into()));
            }
            owned.checkpoint()?;
            if prior.child_private_volume == seed.child_private_volume {
                return Err(Error::Conflict("invalid fork publication".into()));
            }
            for resource in &prior.resources {
                owned.checkpoint()?;
                if matches!((&resource.revision, child_project),
                    (crate::fork::ResourceRevision::Project { volume, .. }, Some(child_project)) if volume == child_project)
                {
                    return Err(Error::Conflict("invalid fork publication".into()));
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn attest_fork_history_with_work(
        &self, seed: &mut ForkSeed, history: &ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        if seed.parent != self.authority || seed.parent_revision != self.revision
            || history.agent != self.conversation.agent
            || history.resident_after_sequence() != 0
            || history.logical_revision() != seed.inherited_through_sequence
        {
            return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Conflict,
                "fork archive differs from its original parent cut"));
        }
        self.validate_fork_reference_ownership_using_with_work(seed, history, work)?;
        seed.history_witness = Some(self.authority_verifier.fork_history_proof(seed, work)?);
        Ok(())
    }

    fn validate_fork_reference_ownership_with_work(
        &self, seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(proof) = seed.history_witness {
            if proof != self.authority_verifier.fork_history_proof(seed, work)? {
                return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Unauthorized,
                    "fork archive witness differs from its exact seed"));
            }
            return Ok(());
        }
        self.validate_fork_reference_ownership_using_with_work(seed, &self.conversation, work)
    }

    #[allow(clippy::too_many_lines, reason = "fork publication checks one complete seed")]
    fn validate_fork_reference_ownership_using_with_work(
        &self,
        seed: &ForkSeed,
        history: &ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut owned = crate::fork::CodecQuota::observed(work)?;
        let inherited_count = usize::try_from(seed.inherited_through_sequence)
            .map_err(|_| Error::Invalid("inherited conversation prefix is too large".into()))?;
        let parent_agent = history.agent
            .ok_or_else(|| Error::Conflict("fork parent conversation is unbound".into()))?;
        if seed.child_agent == parent_agent {
            return Err(Error::Invalid(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        if seed.inherited_through_sequence > history.logical_revision() {
            return Err(Error::Invalid(
                "fork inherited prefix exceeds parent conversation".into(),
            ));
        }
        let mut materialized = None;
        for file in &seed.inherited_context {
            owned.checkpoint()?;
            if file.path() == ".system/inherited-conversation/prefix.json" {
                materialized = Some(file);
                break;
            }
        }
        if seed.inherited_through_sequence > 0 && materialized.is_none() {
            return Err(Error::Invalid(
                "fork omitted its inherited conversation prefix".into(),
            ));
        }
        if let Some(file) = materialized {
            let parent = match owned.work().as_deref_mut() {
                Some(observer) => observer.clone_authority(&self.authority)?,
                None => self.authority.clone(),
            };
            owned.adopt(parent.id.capacity() as u64)?;
            let prefix = InheritedConversationPrefix::select(
                parent,
                self.revision,
                seed.inherited_through_sequence,
                &seed.attached_agents,
                history,
                owned.work(),
            )?;
            owned.adopt(prefix.attached_agents.capacity() as u64 * std::mem::size_of::<AgentId>() as u64)?;
            let bytes = owned.canonical(&prefix)?;
            const PREFIX_MEDIA: &str = "application/vnd.acyclic.harness.inherited-conversation+json";
            if file.descriptor().media_type() != PREFIX_MEDIA
                || file.display_name() != "inherited-conversation.json"
            {
                return Err(Error::Invalid("inherited conversation metadata differs from its parent".into()));
            }
            file.descriptor().verify(&bytes, owned.work())?;
            let byte_capacity = bytes.capacity() as u64;
            let prefix_slots = prefix.parent.id.capacity() as u64
                + prefix.attached_agents.capacity() as u64 * std::mem::size_of::<AgentId>() as u64;
            drop(bytes);
            drop(prefix);
            owned.release(byte_capacity + prefix_slots)?;
        }
        if history.resident_after_sequence() > 0 && inherited_count > 0 {
            return Err(Error::Invalid(
                "fork grant capture requires authenticated archived history".into(),
            ));
        }
        let mut published_refs = std::collections::BTreeMap::new();
        let mut published_manifests = std::collections::BTreeSet::new();
        for message in history.messages.iter().take(inherited_count) {
            owned.checkpoint()?;
            Self::retain_fork_reference(&mut published_refs, &message.content, &mut owned)?;
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items {
                        owned.checkpoint()?;
                        Self::retain_fork_reference(&mut published_refs, &item.file, &mut owned)?;
                    }
                }
                crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
                    let digest = owned.file_digest(manifest)?;
                    Self::retain_fork_digest(&mut published_manifests, digest, &mut owned)?;
                    Self::retain_fork_reference(&mut published_refs, manifest, &mut owned)?;
                }
            }
            for file in message.extensions.values() {
                owned.checkpoint()?;
                Self::retain_fork_reference(&mut published_refs, file, &mut owned)?;
            }
        }
        if let Some(summary) = &seed.summary {
            for file in &summary.references {
                owned.checkpoint()?;
                Self::retain_fork_reference(&mut published_refs, file, &mut owned)?;
            }
        }
        let mut selected_manifests = std::collections::BTreeSet::new();
        for file in &seed.attachment_manifests {
            owned.checkpoint()?;
            let digest = owned.file_digest(file)?;
            Self::retain_fork_digest(&mut selected_manifests, digest, &mut owned)?;
        }
        owned.checkpoint()?;
        let same_manifests = if selected_manifests.len() != published_manifests.len() {
            false
        } else {
            let mut same = true;
            for (selected, published) in selected_manifests.iter().zip(&published_manifests) {
                owned.checkpoint()?;
                if selected != published { same = false; break; }
            }
            same
        };
        if !same_manifests {
            return Err(Error::Invalid(
                "fork attachment manifests do not match the parent transcript".into(),
            ));
        }
        let mut granted = std::collections::BTreeMap::<AgentId, std::collections::BTreeSet<[u8; 32]>>::new();
        for grant in &seed.reference_grants {
            owned.checkpoint()?;
            let digest = owned.file_digest(&grant.file)?;
            let readers = match granted.entry(grant.reader) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    owned.slots::<(AgentId, std::collections::BTreeSet<[u8; 32]>)>(1)?;
                    entry.insert(std::collections::BTreeSet::new())
                }
            };
            Self::retain_fork_digest(readers, digest, &mut owned)?;
        }
        for reader in std::iter::once(seed.child_agent).chain(seed.attached_agents.iter().copied())
        {
            owned.checkpoint()?;
            for (digest, file) in &published_refs {
                owned.checkpoint()?;
                if file.volume().owner() == &crate::conversation::VolumeOwner::Agent(reader) {
                    continue;
                }
                if !granted.get(&reader).is_some_and(|grants| grants.contains(digest)) {
                    return Err(Error::Invalid(
                        "fork omits a published reference grant".into(),
                    ));
                }
            }
        }
        for grant in &seed.reference_grants {
            owned.checkpoint()?;
            let file = &grant.file;
            if file.volume() == &seed.child_private_volume {
                if grant.attachment_manifest.is_some() || {
                    let mut inherited = false;
                    for candidate in &seed.inherited_context {
                        owned.checkpoint()?;
                        if candidate == file { inherited = true; break; }
                    }
                    !inherited
                } {
                    return Err(Error::Invalid(
                        "fork reference is not inherited context".into(),
                    ));
                }
            } else if file.volume().owner()
                == &crate::conversation::VolumeOwner::Agent(parent_agent)
                || {
                    let mut selected = false;
                    for capture in &seed.resources {
                        owned.checkpoint()?;
                        if match &capture.source {
                            crate::fork::ResourceRevision::Project { volume, .. }
                            | crate::fork::ResourceRevision::SharedVolume(volume) => volume == file.volume(),
                            _ => false,
                        } { selected = true; break; }
                    }
                    selected
                }
            {
                let digest = owned.file_digest(file)?;
                let published = published_refs.get(&digest).is_some_and(|published| *published == file);
                match &grant.attachment_manifest {
                    None if !published => {
                        return Err(Error::Invalid(
                            "fork reference was not published by parent".into(),
                        ));
                    }
                    Some(manifest)
                        if {
                            !published_manifests.contains(&owned.file_digest(manifest)?)
                        } =>
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

    fn retain_fork_reference<'a>(
        references: &mut BTreeMap<[u8; 32], &'a FileRef>, file: &'a FileRef,
        owned: &mut crate::fork::CodecQuota<'_, '_>,
    ) -> Result<()> {
        let digest = owned.file_digest(file)?;
        if !references.contains_key(&digest) {
            owned.slots::<([u8; 32], &FileRef)>(1)?;
        }
        references.insert(digest, file);
        Ok(())
    }

    fn retain_fork_digest(
        digests: &mut BTreeSet<[u8; 32]>, digest: [u8; 32],
        owned: &mut crate::fork::CodecQuota<'_, '_>,
    ) -> Result<()> {
        if !digests.contains(&digest) {
            owned.slots::<[u8; 32]>(1)?;
            digests.insert(digest);
        }
        Ok(())
    }

    fn validate_extension_fork_with_work(
        &self, seed: &ForkSeed, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for capture in &seed.resources {
            if let crate::fork::ResourceRevision::Extension {
                name,
                version,
                implementation_digest,
                ..
            } = &capture.revision
            {
                let binding = self.schemas.pinned_binding(name, *version, None, work)?;
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

    #[allow(clippy::too_many_lines, reason = "one arm per action")]
    fn transition_with_work(
        &self,
        action: &Action,
        archived_effect: Option<(EffectId, &EffectState)>,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<EventPayload> {
        match action {
            Action::TransitionLifecycle { to, reason } => {
                validate_lifecycle(self.lifecycle, *to)?;
                Ok(EventPayload::LifecycleTransitioned {
                    from: self.lifecycle,
                    to: *to,
                    reason: reason.as_deref().map(|text| native_custody::copy_text(text, quota)).transpose()?,
                })
            }
            Action::AppendCustom {
                schema,
                version,
                content,
            } => {
                if self
                    .extension_states
                    .get(schema)
                    .is_some_and(|(_, current)| current.version != *version)
                {
                    return Err(Error::Conflict(
                        "extension state version change requires migration".into(),
                    ));
                }
                content.validate(quota.work())?;
                if content.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("extension content must be JSON".into()));
                }
                let binding = self.schemas.pinned_binding(schema, *version, None, quota.work())?;
                let record = ExtensionRecord {
                    name: native_custody::copy_text(schema, quota)?,
                    version: *version,
                    schema_digest: binding.schema_digest,
                    implementation_digest: binding.implementation_digest,
                    fork_policy: binding.fork_policy,
                    content: native_custody::copy_file(content, quota)?,
                };
                record.validate()?;
                Ok(EventPayload::Custom { record })
            }
            Action::MigrateExtensionState {
                name,
                previous,
                to_version,
                content,
            } => {
                let (source, current) = self
                    .extension_states
                    .get(name)
                    .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
                if source != previous
                    || previous.authority != self.authority
                    || current.version == *to_version
                {
                    return Err(Error::Conflict(
                        "extension migration source or target is not exact".into(),
                    ));
                }
                let binding = self.schemas.pinned_binding(name, *to_version, None, quota.work())?;
                let record = ExtensionRecord {
                    name: native_custody::copy_text(name, quota)?,
                    version: *to_version,
                    schema_digest: binding.schema_digest,
                    implementation_digest: binding.implementation_digest,
                    fork_policy: binding.fork_policy,
                    content: native_custody::copy_file(content, quota)?,
                };
                record.validate()?;
                Ok(EventPayload::ExtensionStateMigrated {
                    migration: ExtensionStateMigration {
                        previous: native_custody::copy_reference(previous, quota)?,
                        record,
                    },
                })
            }
            Action::SelectExtensions { roots } => {
                if self.authority.kind != AggregateKind::Agent {
                    return Err(Error::Invalid(
                        "extension selection requires an agent aggregate".into(),
                    ));
                }
                if !matches!(
                    self.lifecycle,
                    LifecycleState::Pending | LifecycleState::Waiting
                ) {
                    return Err(Error::Conflict(
                        "extension selection requires a pending or waiting agent".into(),
                    ));
                }
                let (selected, produced) = self.schemas.activation_order(roots, quota.work())?;
                quota.adopt(produced)?;
                for extension in &selected {
                    if self
                        .extension_states
                        .get(&extension.name)
                        .is_some_and(|(_, state)| state.version != extension.version)
                    {
                        return Err(Error::Conflict(format!(
                            "extension {} state must migrate before activation",
                            extension.name
                        )));
                    }
                }
                let configurations = self.selected_configurations_in_quota(&selected, quota)?;
                Ok(EventPayload::ExtensionsSelected {
                    previous: native_custody::copy_vector(&self.active_extensions, quota, native_custody::copy_dependency)?,
                    selected,
                    configurations,
                })
            }
            Action::ConfigureExtension { extension, content } => {
                if self.authority.kind != AggregateKind::Agent {
                    return Err(Error::Invalid(
                        "extension configuration requires an agent aggregate".into(),
                    ));
                }
                if !matches!(
                    self.lifecycle,
                    LifecycleState::Pending | LifecycleState::Waiting
                ) {
                    return Err(Error::Conflict(
                        "extension configuration requires a pending or waiting agent".into(),
                    ));
                }
                let record = self.schemas.configuration_record_in_quota(extension, content, quota)?;
                Ok(EventPayload::ExtensionConfigured {
                    previous: self.configured_extensions.get(extension).map(|record|
                        native_custody::copy_configuration(record, quota)).transpose()?,
                    record,
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
                request.validate(quota.work())?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                let request_digest =
                    effect_request_digest_with_work(provider, *guarantee, effect_kind, request, quota.work())?;
                result_schema.validate(quota.work())?;
                if result_schema.descriptor().media_type() != "application/schema+json" {
                    return Err(Error::Invalid(
                        "effect result schema must be JSON Schema content".into(),
                    ));
                }
                let result_schema_digest = *result_schema.descriptor().sha256();
                Ok(EventPayload::EffectPlanned {
                    effect_id: *effect_id,
                    provider: native_custody::copy_text(provider, quota)?,
                    guarantee: *guarantee,
                    effect_kind: native_custody::copy_text(effect_kind, quota)?,
                    request: native_custody::copy_file(request, quota)?,
                    request_digest,
                    result_schema: native_custody::copy_file(result_schema, quota)?,
                    result_schema_digest,
                })
            }
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            } => {
                let effect = self.transition_effect(effect_id, archived_effect)?;
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
                let effect = self.transition_effect(&observation.effect_id, archived_effect)?;
                self.authority_verifier.verify_effect(observation, quota.work())?;
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
                    observation: native_custody::copy_observation(observation, quota)?,
                })
            }
            Action::PublishFork { seed } => {
                seed.validate(quota.work())?;
                self.validate_fork_reference_ownership_with_work(seed, quota.work())?;
                self.validate_extension_fork_with_work(seed, quota.work())?;
                if self.forks.contains_key(&seed.child) {
                    return Err(Error::Conflict("fork child is already published".into()));
                }
                Ok(EventPayload::ForkPublished {
                    seed: native_custody::copy_boxed(seed.as_ref(), quota, native_custody::copy_seed)?,
                })
            }
            Action::PublishProjectMerge { receipt } => {
                self.require_conversation()?;
                self.require_bound_conversation_if_applicable()?;
                receipt.validate_shape(quota.work())?;
                let (key, key_owned) = Self::merge_key_with_work(receipt, quota.work())?;
                if self.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                drop(key);
                if let Some(io) = quota.work().as_deref_mut() { io.release_owned(key_owned)?; }
                self.conversation.validate_append(&receipt.notice, &mut None)?;
                Ok(EventPayload::ProjectMergePublished {
                    receipt: native_custody::copy_boxed(receipt.as_ref(), quota, native_custody::copy_receipt)?,
                })
            }
            Action::BindConversation { agent } => {
                self.require_conversation()?;
                if self.conversation.agent.is_some() || !self.conversation.messages.is_empty() {
                    return Err(Error::Conflict("conversation is already bound".into()));
                }
                Ok(EventPayload::ConversationBound { agent: *agent })
            }
            Action::AppendConversationMessage { message } => {
                self.require_conversation()?;
                self.conversation.validate_append(message, &mut None)?;
                Ok(EventPayload::ConversationMessageAppended {
                    message: native_custody::copy_boxed(message.as_ref(), quota, native_custody::copy_message)?,
                })
            }
            Action::SelectModelContext { selection } => {
                self.require_conversation()?;
                selection.validate(&self.conversation)?;
                Ok(EventPayload::ModelContextSelected {
                    selection: native_custody::copy_selection(selection, quota)?,
                })
            }
            Action::OpenInteraction { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate(quota.work())?;
                if self.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                Ok(EventPayload::InteractionOpened {
                    ticket: native_custody::copy_ticket(ticket, quota)?,
                })
            }
            Action::ResolveInteraction { resolution } => {
                let (ticket, prior) = self
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                Ok(EventPayload::InteractionResolved {
                    resolution: native_custody::copy_resolution(resolution, quota)?,
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

    #[allow(clippy::too_many_lines, reason = "one arm per event")]
    fn apply_payload_with_work(
        &mut self, payload: &EventPayload, revision: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
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
                    )), work,
                )?;
                if self
                    .extension_states
                    .get(&record.name)
                    .is_some_and(|(_, current)| current.version != record.version)
                {
                    return Err(Error::Conflict(
                        "extension state version change lacks migration".into(),
                    ));
                }
                self.replace_extension_state_with_work(record, revision, work)?;
            }
            EventPayload::ExtensionStateMigrated { migration } => {
                migration.validate()?;
                let ExtensionStateMigration { previous, record } = migration;
                self.schemas.pinned_binding(
                    &record.name,
                    record.version,
                    Some((
                        record.schema_digest,
                        record.implementation_digest,
                        record.fork_policy,
                    )), work,
                )?;
                let (source, current) = self
                    .extension_states
                    .get(&record.name)
                    .ok_or_else(|| Error::NotFound(format!("extension state {}", record.name)))?;
                if source != previous
                    || previous.authority != self.authority
                    || current.version == record.version
                {
                    return Err(Error::Conflict(
                        "extension migration does not match retained state".into(),
                    ));
                }
                self.replace_extension_state_with_work(record, revision, work)?;
            }
            EventPayload::ExtensionsSelected {
                previous,
                selected,
                configurations,
            } => {
                if self.authority.kind != AggregateKind::Agent
                    || !matches!(
                        self.lifecycle,
                        LifecycleState::Pending | LifecycleState::Waiting
                    )
                    || previous != &self.active_extensions
                    || !self.selected_configuration_matches_with_work(selected, configurations, work)?
                    || selected.iter().any(|extension| {
                        self.extension_states
                            .get(&extension.name)
                            .is_some_and(|(_, state)| state.version != extension.version)
                    })
                {
                    return Err(Error::Conflict(
                        "extension activation does not match pinned agent state".into(),
                    ));
                }
                let (next_selected, retired_selected) = self.native_custody.produce(
                    &NativeComponentKey::ActiveExtensions, work,
                    |quota| native_custody::copy_vector(selected, quota, native_custody::copy_dependency),
                )?;
                let copied_configurations = self.native_custody.produce(
                    &NativeComponentKey::ActiveConfigurations, work,
                    |quota| native_custody::copy_vector(configurations, quota, native_custody::copy_configuration),
                );
                let (next_configurations, retired_configurations) = match copied_configurations {
                    Ok(value) => value,
                    Err(error) => {
                        drop(next_selected);
                        drop(self.native_custody.rollback(&NativeComponentKey::ActiveExtensions, retired_selected));
                        return Err(error);
                    }
                };
                self.active_extension_revision = Some(revision);
                self.active_extensions = next_selected;
                self.active_configurations = next_configurations;
                drop(retired_configurations);
                drop(retired_selected);
            }
            EventPayload::ExtensionConfigured { previous, record } => {
                if self.authority.kind != AggregateKind::Agent
                    || !matches!(
                        self.lifecycle,
                        LifecycleState::Pending | LifecycleState::Waiting
                    )
                    || self.configured_extensions.get(&record.extension) != previous.as_ref()
                    || {
                        record.validate()?;
                        self.schemas.pinned_binding(&record.extension.name, record.extension.version, None, work)?
                            .configuration_schema_digest != Some(record.schema_digest)
                    }
                {
                    return Err(Error::Conflict(
                        "extension configuration does not match pinned agent state".into(),
                    ));
                }
                let (entry, _retired) = self.native_custody.produce(
                    &NativeComponentKey::ExtensionConfiguration(Cow::Borrowed(&record.extension)), work,
                    |quota| {
                        native_custody::retain_entry::<(ExtensionDependency, ExtensionConfiguration)>(quota)?;
                        let key = native_custody::copy_dependency(&record.extension, quota)?;
                        let record = native_custody::copy_configuration(record, quota)?;
                        Ok((key, record))
                    },
                )?;
                if work.as_deref().is_some_and(native_custody::is_native) {
                    drop(self.configured_extensions.remove_entry(&record.extension));
                }
                self.configured_extensions.insert(entry.0, entry.1);
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
                request.validate(work)?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                if self.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if *request_digest
                    != effect_request_digest_with_work(provider, *guarantee, effect_kind, request, work)?
                {
                    return Err(Error::Invalid("effect request digest mismatch".into()));
                }
                result_schema.validate(work)?;
                if result_schema.descriptor().media_type() != "application/schema+json"
                    || *result_schema_digest != *result_schema.descriptor().sha256()
                {
                    return Err(Error::Invalid(
                        "effect result schema digest mismatch".into(),
                    ));
                }
                let (effect, _retired) = self.native_custody.produce(
                    &NativeComponentKey::EffectAction(*effect_id), work,
                    |quota| {
                        native_custody::retain_entry::<(EffectId, EffectState)>(quota)?;
                        Ok(EffectState {
                            provider: native_custody::copy_text(provider, quota)?, guarantee: *guarantee,
                            effect_kind: native_custody::copy_text(effect_kind, quota)?,
                            request: native_custody::copy_file(request, quota)?, request_digest: *request_digest,
                            result_schema: native_custody::copy_file(result_schema, quota)?,
                            result_schema_digest: *result_schema_digest,
                            status: EffectStatus::Planned, attempts: Vec::new(),
                        })
                    },
                )?;
                self.effects.insert(*effect_id, effect);
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
                if work.as_deref().is_some_and(native_custody::is_native)
                    && effect.attempts.len() == effect.attempts.capacity()
                {
                    let maximum = effect.attempts.capacity().max(1).checked_mul(2)
                        .ok_or_else(|| Error::Invalid("effect attempt capacity overflows".into()))?;
                    let copied = effect.attempts.len().checked_mul(std::mem::size_of::<EffectAttemptId>())
                        .ok_or_else(|| Error::Invalid("effect attempt move bytes overflow".into()))? as u64;
                    work.as_deref().expect("attempt copy observer").admit_copy(copied)?;
                    let (mut replacement, retired) = self.native_custody.produce(
                        &NativeComponentKey::EffectAttempts(*effect_id), work,
                        |quota| native_custody::reserve_copy_vector::<EffectAttemptId>(maximum, quota),
                    )?;
                    if let Err(error) = work.as_deref_mut().expect("attempt copy observer").copy_bytes(copied) {
                        drop(replacement);
                        drop(self.native_custody.rollback(&NativeComponentKey::EffectAttempts(*effect_id), retired));
                        return Err(error);
                    }
                    replacement.append(&mut effect.attempts);
                    let old_payload = std::mem::replace(&mut effect.attempts, replacement);
                    drop(old_payload);
                    drop(retired);
                }
                effect.attempts.push(*attempt_id);
                effect.status = EffectStatus::Dispatched;
                self.native_custody.remove(&NativeComponentKey::EffectStatus(*effect_id));
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .effects
                    .get_mut(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                self.authority_verifier.verify_effect(observation, work)?;
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
                let (status, _retired) = self.native_custody.produce(
                    &NativeComponentKey::EffectStatus(observation.effect_id), work,
                    |quota| native_custody::copy_status(&observation.status, quota),
                )?;
                effect.status = status;
            }
            EventPayload::ForkPublished { seed } => {
                seed.validate(work)?;
                self.validate_extension_fork_with_work(seed, work)?;
                self.require_fresh_fork_with_work(seed, work)?;
                if let Some(observer) = work.as_deref_mut().filter(|io| !native_custody::is_native(&**io)) {
                    let reservation = snapshot::source_inventory(seed)?
                        .checked_add(seed.child.id.len() as u64)
                        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<(Authority, ForkSeed)>() as u64))
                        .ok_or_else(|| Error::Invalid("fork map clone quota overflows".into()))?;
                    observer.admit_owned(reservation)?;
                    observer.retain_owned(reservation)?;
                }
                let (entry, _retired) = self.native_custody.produce(
                    &NativeComponentKey::Fork(Cow::Borrowed(&seed.child)), work,
                    |quota| {
                        native_custody::retain_entry::<(Authority, ForkSeed)>(quota)?;
                        let child = native_custody::copy_authority(&seed.child, quota)?;
                        let seed = native_custody::copy_seed(seed, quota)?;
                        Ok((child, seed))
                    },
                )?;
                self.forks.insert(entry.0, entry.1);
            }
            EventPayload::ProjectMergePublished { receipt } => {
                self.require_conversation()?;
                receipt.validate_shape(work)?;
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let (key, key_owned) = Self::merge_key_with_work(receipt, quota.work())?;
                quota.adopt(key_owned)?;
                if self.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(native_custody::is_native) {
                    native_custody::retain_entry::<(String, Vec<u8>)>(&mut quota)?;
                }
                let retired = self.native_custody.install(NativeComponentCustody::prepare(
                    &NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)), 0, quota.work(),
                )?, quota.work())?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(native_custody::is_native) {
                    let produced = quota.retained();
                    quota.transfer();
                    if let Err(error) = self.native_custody.absorb_produced(
                        &NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)), produced,
                        quota.work().as_deref_mut().expect("merge producing observer"),
                    ) {
                        let incoming = self.native_custody.rollback(&NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)), retired);
                        drop(key);
                        drop(incoming);
                        return Err(error);
                    }
                }
                if let Err(error) = self.append_conversation_message_with_work(&receipt.notice, quota.work()) {
                    let incoming = self.native_custody.rollback(&NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)), retired);
                    drop(key);
                    drop(incoming);
                    return Err(error);
                }
                self.published_merges.insert(key);
                drop(retired);
            }
            EventPayload::ConversationBound { agent } => {
                self.require_conversation()?;
                self.conversation.bind(*agent)?;
            }
            EventPayload::ConversationMessageAppended { message } => {
                self.require_conversation()?;
                self.append_conversation_message_with_work(message, work)?;
            }
            EventPayload::ModelContextSelected { selection } => {
                self.require_conversation()?;
                selection.validate(&self.conversation)?;
                if let Some(checkpoint) = &selection.checkpoint {
                    let (checkpoint, _retired) = self.native_custody.produce(
                        &NativeComponentKey::ContextCheckpoint, work,
                        |quota| native_custody::copy_file(checkpoint, quota),
                    )?;
                    self.latest_context_checkpoint = Some(checkpoint);
                }
            }
            EventPayload::InteractionOpened { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate(work)?;
                if self.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                let (ticket, _retired) = self.native_custody.produce(
                    &NativeComponentKey::InteractionTicket(ticket.id), work,
                    |quota| {
                        native_custody::retain_entry::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>(quota)?;
                        native_custody::copy_ticket(ticket, quota)
                    },
                )?;
                self.interactions.insert(ticket.id, (ticket, None));
            }
            EventPayload::InteractionResolved { resolution } => {
                let (ticket, prior) = self
                    .interactions
                    .get_mut(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                let (resolution, _retired) = self.native_custody.produce(
                    &NativeComponentKey::InteractionResolution(resolution.id), work,
                    |quota| native_custody::copy_resolution(resolution, quota),
                )?;
                *prior = Some(resolution);
            }
        }
        Ok(())
    }
}

/// Declares [`TransitionKind`] from one table: each transition's serde `kind` tag as an [`Action`], as an [`EventPayload`], and the capability both require.
macro_rules! transition_kinds {
    ($($kind:ident => $action:literal, $event:literal, $capability:ident;)*) => {
        /// One reducer transition, shared by its command and its committed event.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) enum TransitionKind {
            $($kind,)*
        }

        impl TransitionKind {
            #[cfg(test)]
            const ALL: &[Self] = &[$(Self::$kind,)*];

            const fn spec(self) -> (&'static str, &'static str, &'static str) {
                match self {
                    $(Self::$kind => ($action, $event, capability::$capability),)*
                }
            }
        }
    };
}

transition_kinds! {
    Lifecycle => "transition_lifecycle", "lifecycle_transitioned", LIFECYCLE_MANAGE;
    Custom => "append_custom", "custom", EVENT_APPEND;
    Migration => "migrate_extension_state", "extension_state_migrated", EXTENSION_MIGRATE;
    Selection => "select_extensions", "extensions_selected", EXTENSION_ACTIVATE;
    Configuration => "configure_extension", "extension_configured", EXTENSION_CONFIGURE;
    PlanEffect => "plan_effect", "effect_planned", EFFECT_RUN;
    DispatchEffect => "mark_effect_dispatched", "effect_dispatched", EFFECT_RUN;
    ResolveEffect => "resolve_effect", "effect_resolved", EFFECT_RUN;
    Fork => "publish_fork", "fork_published", FORK_PUBLISH;
    ProjectMerge => "publish_project_merge", "project_merge_published", PROJECT_MERGE;
    BindConversation => "bind_conversation", "conversation_bound", CONVERSATION_BIND;
    AppendMessage => "append_conversation_message", "conversation_message_appended", CONVERSATION_APPEND;
    SelectContext => "select_model_context", "model_context_selected", CONVERSATION_SELECT_CONTEXT;
    OpenInteraction => "open_interaction", "interaction_opened", INTERACTION_OPEN;
    ResolveInteraction => "resolve_interaction", "interaction_resolved", INTERACTION_RESOLVE;
}

/// Transition input whose authorization depends on reducer state.
enum Subject<'a> {
    None,
    PlannedEffect,
    Effect(&'a EffectId),
    Interaction(&'a InteractionResolution),
    Fork(&'a ForkSeed),
    Merge(&'a ProjectMergeReceipt),
}

impl Action {
    fn kind(&self) -> (TransitionKind, Subject<'_>) {
        use TransitionKind as Kind;
        match self {
            Self::TransitionLifecycle { .. } => (Kind::Lifecycle, Subject::None),
            Self::AppendCustom { .. } => (Kind::Custom, Subject::None),
            Self::MigrateExtensionState { .. } => (Kind::Migration, Subject::None),
            Self::SelectExtensions { .. } => (Kind::Selection, Subject::None),
            Self::ConfigureExtension { .. } => (Kind::Configuration, Subject::None),
            Self::PlanEffect { .. } => (Kind::PlanEffect, Subject::PlannedEffect),
            Self::MarkEffectDispatched { effect_id, .. } => {
                (Kind::DispatchEffect, Subject::Effect(effect_id))
            }
            Self::ResolveEffect { observation } => {
                (Kind::ResolveEffect, Subject::Effect(&observation.effect_id))
            }
            Self::PublishFork { seed } => (Kind::Fork, Subject::Fork(seed)),
            Self::PublishProjectMerge { receipt } => (Kind::ProjectMerge, Subject::Merge(receipt)),
            Self::BindConversation { .. } => (Kind::BindConversation, Subject::None),
            Self::AppendConversationMessage { .. } => (Kind::AppendMessage, Subject::None),
            Self::SelectModelContext { .. } => (Kind::SelectContext, Subject::None),
            Self::OpenInteraction { .. } => (Kind::OpenInteraction, Subject::None),
            Self::ResolveInteraction { resolution } => {
                (Kind::ResolveInteraction, Subject::Interaction(resolution))
            }
        }
    }

    /// Serde `kind` tag, carried beside canonical action JSON on the wire.
    #[cfg_attr(
        not(all(feature = "wasm", target_arch = "wasm32")),
        allow(dead_code, reason = "only the WASM command decoder reads it")
    )]
    pub(crate) fn tag(&self) -> &'static str {
        self.kind().0.spec().0
    }
}

impl EventPayload {
    fn kind(&self) -> (TransitionKind, Subject<'_>) {
        use TransitionKind as Kind;
        match self {
            Self::LifecycleTransitioned { .. } => (Kind::Lifecycle, Subject::None),
            Self::Custom { .. } => (Kind::Custom, Subject::None),
            Self::ExtensionStateMigrated { .. } => (Kind::Migration, Subject::None),
            Self::ExtensionsSelected { .. } => (Kind::Selection, Subject::None),
            Self::ExtensionConfigured { .. } => (Kind::Configuration, Subject::None),
            Self::EffectPlanned { .. } => (Kind::PlanEffect, Subject::PlannedEffect),
            Self::EffectDispatched { effect_id, .. } => {
                (Kind::DispatchEffect, Subject::Effect(effect_id))
            }
            Self::EffectResolved { observation } => {
                (Kind::ResolveEffect, Subject::Effect(&observation.effect_id))
            }
            Self::ForkPublished { seed } => (Kind::Fork, Subject::Fork(seed)),
            Self::ProjectMergePublished { receipt } => {
                (Kind::ProjectMerge, Subject::Merge(receipt))
            }
            Self::ConversationBound { .. } => (Kind::BindConversation, Subject::None),
            Self::ConversationMessageAppended { .. } => (Kind::AppendMessage, Subject::None),
            Self::ModelContextSelected { .. } => (Kind::SelectContext, Subject::None),
            Self::InteractionOpened { .. } => (Kind::OpenInteraction, Subject::None),
            Self::InteractionResolved { resolution } => {
                (Kind::ResolveInteraction, Subject::Interaction(resolution))
            }
        }
    }

    /// Serde `kind` tag, carried beside canonical payload JSON on the wire.
    pub(crate) fn tag(&self) -> &'static str {
        self.kind().0.spec().1
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
    let expected = prior.map_or(Ok(1_u64), |value| next_revision(value.expected_version))?;
    if resolution.expected_version != expected {
        return Err(Error::Conflict(
            "interaction resolution version mismatch".into(),
        ));
    }
    Ok(())
}

fn validate_interaction_operation(
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

fn validate_causal_parent_with_work(
    authority: &Authority,
    current_revision: u64,
    reference: Option<&EventReference>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let Some(reference) = reference else {
        return Ok(());
    };
    if reference.revision == 0 {
        return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Invalid,
            "causal event revision must be positive"));
    }
    if reference.authority == *authority && reference.revision > current_revision {
        return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Invalid,
            "causal event is in the future"));
    }
    reference.authority.validate_with_work(work)?;
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

#[allow(clippy::too_many_arguments, reason = "the original closed signed-grant fields")]
fn scope_proof(
    key: &[u8; 32],
    audience: &Authority,
    issuer: &str,
    id: &str,
    capabilities: &Capabilities,
    agent: Option<AgentId>,
    parent_proof: Option<[u8; 32]>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    if let Some(work) = work.as_deref_mut() {
        let fixed = 1_u64.checked_add(8)
            .and_then(|bytes| bytes.checked_add(audience.id.len() as u64))
            .and_then(|bytes| bytes.checked_add(8 + issuer.len() as u64))
            .and_then(|bytes| bytes.checked_add(8 + id.len() as u64))
            .and_then(|bytes| bytes.checked_add(1 + u64::from(agent.is_some()) * 16))
            .and_then(|bytes| bytes.checked_add(1 + u64::from(parent_proof.is_some()) * 32))
            .ok_or_else(|| Error::Invalid("scope hash byte count overflows".into()))?;
        let bytes = capabilities.iter().try_fold(fixed, |total, capability| {
            total.checked_add(8 + capability.len() as u64)
                .ok_or_else(|| Error::Invalid("scope hash byte count overflows".into()))
        })?;
        work.hashed_bytes(bytes)?;
    }
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
    Ok(*hasher.finalize().as_bytes())
}

#[allow(clippy::too_many_arguments, reason = "each argument is one field of the original attested proof preimage")]
fn effect_attestation_proof_with_work(
    key: &[u8; 32],
    audience: &Authority,
    provider: &str,
    effect_id: EffectId,
    attempt_id: EffectAttemptId,
    request_digest: [u8; 32],
    guarantee: EffectGuarantee,
    result_schema_digest: [u8; 32],
    status: &EffectStatus,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let bytes = crate::store::history::canonical_bytes_with_work(&(
        audience,
        provider,
        effect_id,
        attempt_id,
        request_digest,
        guarantee,
        result_schema_digest,
        status,
    ), crate::store::default_projection_read_limits().maximum_bytes, custody.work())?;
    custody.adopt(bytes.capacity() as u64)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(bytes.len() as u64)?;
    }
    let proof = *blake3::keyed_hash(key, &bytes).as_bytes();
    drop(bytes);
    Ok(proof)
}

fn effect_request_digest_with_work(
    provider: &str, guarantee: EffectGuarantee, effect_kind: &str, request: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest_with_work(
        &(provider, guarantee, effect_kind, request),
        acyclic_stream::MAX_RECORD_BYTES as u64, work,
    )
}

fn validate_effect_result(status: &EffectStatus) -> Result<()> {
    if let EffectStatus::Succeeded { result } = status {
        result.validate(&mut None)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::FileDescriptor;
    use serde_json::json;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn original_proof_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            peak_allocation_bytes: 16_777_216,
            bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216,
            source_bytes_read: 16_777_216,
            items_examined: 16_777_216,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_snapshot_round_trip(reducer: &Reducer, issuer: &AuthorityIssuer, maximum_events: usize) -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let source_token = CancellationToken::new();
        let source = NativeMaterializationWork::new(original_proof_budget(), &source_token).expect("snapshot ledger");
        let held = reducer.snapshot(maximum_events, &mut Some(&mut crate::store::history::native_work_io(&source).map_err(Error::from)?))?.0;
        let copy_token = CancellationToken::new();
        let copy = NativeMaterializationWork::new(original_proof_budget(), &copy_token).expect("copied payload ledger");
        let mut io = crate::store::history::native_work_io(&copy).map_err(Error::from)?;
        let input = held.copy(&mut Some(&mut io))?;
        drop(io);
        let copy_receipt = copy.receipt();
        let metadata_token = CancellationToken::new();
        let metadata = NativeMaterializationWork::new(original_proof_budget(), &metadata_token).expect("restore metadata ledger");
        let mut restored = Reducer::restore_native(input, issuer.verifier(&mut None)?, reducer.schemas.clone(), &metadata)?;
        restored.set_resident_event_limit(maximum_events)?;
        assert_eq!(&restored, reducer);
        assert_eq!(copy.receipt(), copy_receipt);
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        assert!(copy.admit_retained(available).is_err());
        assert!(metadata.admit_retained(available).is_err());
        drop(restored);
        copy.admit_retained(available).map_err(Error::from)?;
        metadata.admit_retained(available).map_err(Error::from)?;
        assert!(source.admit_retained(available).is_err());
        drop(held);
        source.admit_retained(available).map_err(Error::from)?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_component_event(reference: &mut Reducer, action: Action) -> Result<Event> {
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&(reference.revision() + 1).to_le_bytes());
        let result = reference.apply(command(OperationId::from_bytes(id), reference.revision(), action))?;
        let ApplyResult::Applied { event } = result else { panic!("fresh canonical model event") };
        reference.mark_indexed_terminal_effect_with_work(&event, &mut None)?;
        Ok(event)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_component_apply(
        state: &mut Reducer, event: Event, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work).expect("original native observer");
        let result = state.apply_committed(event, &mut Some(&mut io))?;
        let ApplyResult::Applied { event } = result else { panic!("fresh canonical model application") };
        state.mark_indexed_terminal_effect_with_work(&event, &mut Some(&mut io))?;
        drop(event);
        drop(io);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_component_fixture(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Reducer, Reducer)> {
        use crate::store::history::HistoryIo;
        let owner = issuer();
        let registry = schemas();
        let mut reference = Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), registry.clone());
        reference.set_resident_event_limit(1)?;
        let mut io = crate::store::history::native_work_io(work).expect("original constructor observer");
        let authority = io.clone_authority(&owner.audience)?;
        let verifier = owner.verifier(&mut Some(&mut io))?;
        let shared_registry = registry.copy(&mut Some(&mut io))?;
        let mut state = Reducer::new_with_preclaimed_work(
            authority, verifier, shared_registry, &mut Some(&mut io),
        )?;
        drop(io);
        state.set_resident_event_limit(1)?;
        let event = native_component_event(&mut reference, Action::AppendCustom {
            schema: "example.message".into(), version: 1,
            content: request_file(br#"{"text":"original primed entry"}"#)?,
        })?;
        native_component_apply(&mut state, event, work)?;
        assert_eq!(state, reference);
        Ok((state, reference))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_component_clone_cold_restore_and_held_snapshot_survive_real_replacement_and_prune() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let prime_token = CancellationToken::new();
        let prime = NativeMaterializationWork::new(original_proof_budget(), &prime_token).expect("prime ledger");
        let (mut state, mut reference) = native_component_fixture(&prime)?;
        let snapshot_token = CancellationToken::new();
        let snapshot_work = NativeMaterializationWork::new(original_proof_budget(), &snapshot_token).expect("held snapshot ledger");
        let held = state.snapshot(1, &mut Some(&mut crate::store::history::native_work_io(&snapshot_work).map_err(|cause| Error::NativeWork { cause, custody: None })?))?.0;
        let old_revision = held.revision;
        let clone_token = CancellationToken::new();
        let clone_work = NativeMaterializationWork::new(original_proof_budget(), &clone_token).expect("independent reducer clone ledger");
        let mut clone_io = crate::store::history::native_work_io(&clone_work).expect("clone observer");
        let cloned = state.copy(&mut Some(&mut clone_io))?;
        assert_eq!(cloned, state);
        assert_eq!(clone_io.into_custody().bytes(), 0, "live copied components own every produced claim");
        let wire = crate::contract::canonical_json_bytes(&held)?;
        let cold_token = CancellationToken::new();
        let cold_work = NativeMaterializationWork::new(original_proof_budget(), &cold_token).expect("cold decoder ledger");
        let cold_baseline = cold_work.live_allocation_bytes();
        let mut cold_io = crate::store::history::native_work_io(&cold_work).expect("cold observer");
        let decoded = Snapshot::decode(&wire, wire.len() as u64, &mut Some(&mut cold_io))?;
        assert_eq!(cold_io.into_custody().bytes(), 0);
        cold_token.cancel();
        let cold_receipt = cold_work.receipt();
        let restore_token = CancellationToken::new();
        let restore_work = NativeMaterializationWork::new(original_proof_budget(), &restore_token).expect("restore metadata ledger");
        let mut restored = Reducer::restore_native(decoded, issuer().verifier(&mut None).expect("verifier copy"), schemas(), &restore_work)?;
        restored.set_resident_event_limit(1)?;
        assert_eq!(restored, state);
        assert_eq!(cold_work.receipt(), cold_receipt, "restore never spends on the independent source ledger");
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        for index in 0..3 {
            let token = CancellationToken::new();
            let work = NativeMaterializationWork::new(original_proof_budget(), &token).expect("replacement query ledger");
            let event = native_component_event(&mut reference, Action::AppendCustom {
                schema: "example.message".into(), version: 1,
                content: request_file(format!(r#"{{"text":"replacement {index}"}}"#).as_bytes())?,
            })?;
            native_component_apply(&mut state, event.clone(), &work)?;
            native_component_apply(&mut restored, event, &work)?;
            assert_eq!(state, reference);
            assert_eq!(restored, state);
            assert_eq!(held.revision, old_revision);
            assert_eq!(cloned.revision(), old_revision);
            assert_eq!(held.events[0].revision, old_revision);
        }
        assert!(snapshot_work.admit_retained(available).is_err());
        assert!(clone_work.admit_retained(available).is_err());
        assert!(cold_work.live_allocation_bytes() > cold_baseline);
        drop((state, restored));
        assert_eq!(cold_work.live_allocation_bytes(), cold_baseline);
        restore_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(snapshot_work.admit_retained(available).is_err(), "held snapshot survives both reducer drops");
        drop(held);
        snapshot_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(clone_work.admit_retained(available).is_err(), "independent reducer copy remains live");
        drop(cloned);
        clone_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_midapply_denial_drops_partial_copy_and_preserves_unreplaced_source() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let prime_token = CancellationToken::new();
        let prime = NativeMaterializationWork::new(original_proof_budget(), &prime_token).expect("prime ledger");
        let (mut measured, mut reference) = native_component_fixture(&prime)?;
        let event = native_component_event(&mut reference, Action::AppendCustom {
            schema: "example.message".into(), version: 1,
            content: request_file(br#"{"text":"real partially applied replacement"}"#)?,
        })?;
        let success_token = CancellationToken::new();
        let success = NativeMaterializationWork::new(original_proof_budget(), &success_token).expect("measured original ledger");
        native_component_apply(&mut measured, event.clone(), &success)?;
        let mut allowance = original_proof_budget();
        allowance.allocation_operations = success.receipt().allocation_operations - 1;
        let other_prime_token = CancellationToken::new();
        let other_prime = NativeMaterializationWork::new(original_proof_budget(), &other_prime_token).expect("identical source fixture ledger");
        let (mut state, _) = native_component_fixture(&other_prime)?;
        let source_head = state.events.front().expect("actual old source head").clone();
        let source_receipt = other_prime.receipt();
        let token = CancellationToken::new();
        let denied = NativeMaterializationWork::new(allowance, &token).expect("real late-phase denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("original denial observer");
        let error = state.apply_committed(event, &mut Some(&mut io)).expect_err("last physical field copy exceeds the measured limit");
        assert!(matches!(&error, Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { counter: "allocation_operations", .. }), ..
        }));
        drop(error);
        drop(io);
        assert_eq!(state.extension_states.get("example.message").expect("old live state").0.revision, source_head.revision,
            "failed component copy never installs its partial fields");
        assert_eq!(state.events.front(), Some(&source_head), "failed apply preserves the old physical event");
        assert_eq!(state.revision(), source_head.revision);
        assert_eq!(other_prime.receipt(), source_receipt);
        let available = allowance.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        denied.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(other_prime.admit_retained(available).is_err(), "unreplaced original cached event and buffers remain owned");
        drop(state);
        denied.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        other_prime.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_component_model_10000_real_signed_transitions_stays_live_source_bounded() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let prime_token = CancellationToken::new();
        let prime = NativeMaterializationWork::new(original_proof_budget(), &prime_token).expect("prime ledger");
        let (mut state, mut reference) = native_component_fixture(&prime)?;
        state.set_resident_event_limit(4)?;
        reference.set_resident_event_limit(4)?;
        state.set_resident_terminal_effect_limit(2)?;
        reference.set_resident_terminal_effect_limit(2)?;
        let owner = issuer();
        let request = request_file(br#"{"prompt":"actual provider fixture request"}"#)?;
        let result_schema = schema_file(&json!({"type":"object"}))?;
        let custom = request_file(br#"{"text":"steady active extension state"}"#)?;
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        let mut recent = VecDeque::new();
        for index in 0_u64..10_000 {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(&(index / 4 + 1).to_le_bytes());
            let effect_id = EffectId::from_bytes(id);
            let attempt_id = EffectAttemptId::from_bytes(id);
            let action = match index % 4 {
                0 => Action::AppendCustom {
                    schema: "example.message".into(), version: 1, content: custom.clone(),
                },
                1 => Action::PlanEffect {
                    effect_id, provider: "example.provider".into(),
                    guarantee: EffectGuarantee::AtMostOnce, effect_kind: "example.model_output".into(),
                    request: request.clone(), result_schema: result_schema.clone(),
                },
                2 => Action::MarkEffectDispatched { effect_id, attempt_id },
                _ => Action::ResolveEffect { observation: owner.attest_effect(
                    effect_id, reference.effect(effect_id).expect("real admitted provider effect"),
                    attempt_id, EffectStatus::Failed { message: "actual attested provider rejection".into() },
                    &mut None,
                )? },
            };
            let event = native_component_event(&mut reference, action)?;
            let token = CancellationToken::new();
            let query = NativeMaterializationWork::new(original_proof_budget(), &token).expect("independent original query ledger");
            native_component_apply(&mut state, event, &query)?;
            let receipt = query.receipt();
            assert!(receipt.allocation_operations < 512 && receipt.bytes_copied < 1_048_576,
                "one active transition must not materialize its 10k lifetime prefix");
            assert_eq!(state, reference, "canonical provider/effect state diverged at transition {index}");
            assert!(state.events.len() <= 4 && state.operation_positions.len() <= 4);
            assert!(state.effects.len() <= 3 && state.indexed_terminal_effects.len() <= 2);
            assert_eq!(state.extension_states.len(), 1);
            recent.push_back((index, query));
            if recent.len() > 16 {
                let (retired_index, retired) = recent.pop_front().expect("bounded observation window");
                if retired_index >= 64 {
                    retired.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                }
            }
        }
        assert_eq!(state.revision(), 10_001);
        let mut oldest_effect = [0_u8; 16];
        oldest_effect[..8].copy_from_slice(&1_u64.to_le_bytes());
        assert!(state.effect(EffectId::from_bytes(oldest_effect)).is_none(), "the first actual provider effect was physically retired");
        drop(state);
        for (_, work) in recent {
            work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        Ok(())
    }

    /// One generated reducer input; indices select earlier operations.
    #[derive(Clone, Debug)]
    enum Step {
        Lifecycle(usize),
        Custom(u8),
        Bind(u8),
        Resubmit(usize),
        ConflictingIntent(usize),
        StaleRevision,
    }

    fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        prop_oneof![
            (0..6_usize).prop_map(Step::Lifecycle),
            any::<u8>().prop_map(Step::Custom),
            any::<u8>().prop_map(Step::Bind),
            any::<usize>().prop_map(Step::Resubmit),
            any::<usize>().prop_map(Step::ConflictingIntent),
            Just(Step::StaleRevision),
        ]
    }

    fn pick_admitted(
        admitted: &[(Command, Event)],
        pick: usize,
    ) -> std::result::Result<&(Command, Event), proptest::test_runner::TestCaseError> {
        admitted
            .get(pick % admitted.len())
            .ok_or_else(|| proptest::test_runner::TestCaseError::fail("no admitted operation"))
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        #[test]
        fn reducer_admission_is_replayable_and_atomic(
            steps in proptest::collection::vec(step(), 1..24),
        ) {
            use proptest::prelude::*;
            let fail = |error: Error| TestCaseError::fail(error.to_string());
            let lifecycle = [
                LifecycleState::Pending,
                LifecycleState::Active,
                LifecycleState::Waiting,
                LifecycleState::Completed,
                LifecycleState::Failed,
                LifecycleState::Cancelled,
            ];
            let authority = issuer().verifier(&mut None).expect("verifier copy");
            let mut reducer = Reducer::new(
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "conversation-1".into(),
                },
                authority.clone(),
                schemas(),
            );
            let mut admitted: Vec<(Command, Event)> = Vec::new();
            for (index, step) in steps.into_iter().enumerate() {
                let id = OperationId::from_bytes(
                    u128::try_from(index + 1).unwrap_or_default().to_be_bytes(),
                );
                let revision = reducer.revision();
                let next = match step {
                    Step::Lifecycle(to) => command(id, revision, Action::TransitionLifecycle {
                        to: *lifecycle.get(to).ok_or_else(|| TestCaseError::fail("state"))?,
                        reason: None,
                    }),
                    Step::Custom(text) => command(id, revision, Action::AppendCustom {
                        schema: "example.message".into(),
                        version: 1,
                        content: request_file(format!(r#"{{"text":"{text}"}}"#).as_bytes())
                            .map_err(fail)?,
                    }),
                    Step::Bind(agent) => command(id, revision, Action::BindConversation {
                        agent: AgentId::from_bytes([agent; 16]),
                    }),
                    Step::Resubmit(_) | Step::ConflictingIntent(_) if admitted.is_empty() => {
                        continue;
                    }
                    Step::Resubmit(pick) => {
                        let (prior, event) = pick_admitted(&admitted, pick)?;
                        let replayed = reducer.apply(prior.clone()).map_err(fail)?;
                        prop_assert_eq!(replayed, ApplyResult::Replayed { event: event.clone() });
                        continue;
                    }
                    Step::ConflictingIntent(pick) => {
                        let (prior, _) = pick_admitted(&admitted, pick)?;
                        let conflicting = command(prior.operation_id, revision, Action::TransitionLifecycle {
                            to: LifecycleState::Active,
                            reason: Some("other intent".into()),
                        });
                        let before = reducer.copy(&mut None).map_err(fail)?;
                        prop_assert!(matches!(
                            reducer.apply(conflicting),
                            Err(Error::Conflict(_))
                        ));
                        prop_assert_eq!(&reducer, &before);
                        continue;
                    }
                    Step::StaleRevision => command(
                        id,
                        revision.wrapping_add(1),
                        Action::TransitionLifecycle {
                            to: LifecycleState::Active,
                            reason: None,
                        },
                    ),
                };
                let before = reducer.copy(&mut None).map_err(fail)?;
                let planned = reducer.plan(&next);
                prop_assert_eq!(&reducer, &before);
                match planned {
                    Ok(ApplyResult::Applied { event }) => {
                        prop_assert_eq!(event.revision, revision + 1);
                        let committed = reducer.apply_committed(event.clone(), &mut None).map_err(fail)?;
                        prop_assert_eq!(committed, ApplyResult::Applied { event: event.clone() });
                        prop_assert_eq!(reducer.revision(), revision + 1);
                        admitted.push((next, event));
                    }
                    Ok(ApplyResult::Replayed { .. }) => {
                        prop_assert!(false, "fresh operation replayed");
                    }
                    Err(_) => {
                        prop_assert!(reducer.apply(next).is_err());
                        prop_assert_eq!(&reducer, &before);
                    }
                }
            }
            let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None).map(|(snapshot, _)| snapshot).map_err(fail)?)
                .map_err(fail)?;
            let decoded = crate::contract::json_from_slice(&wire)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let restored = Reducer::restore(
                decoded,
                authority.clone(),
                schemas(),
            )
            .map_err(fail)?;
            let mut replayed = Reducer::new(reducer.authority().clone(), authority, schemas());
            for (_, event) in &admitted {
                replayed.apply_committed(event.clone(), &mut None).map_err(fail)?;
            }
            prop_assert_eq!(&restored, &reducer);
            prop_assert_eq!(&replayed, &reducer);
        }
    }

    #[test]
    fn v2_extension_admission_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/extension-admission.json").trim();
        let record: ExtensionAdmission =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        record.validate(&mut None)?;
        let encoded =
            serde_json::to_string(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn extension_activation_requires_validated_configuration_and_pins_revision() -> Result<()> {
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "agent-configuration".into(),
        };
        let issuer = AuthorityIssuer::new("extension-host", [45; 32], authority.clone());
        let extension = ExtensionDependency {
            name: "example.configured".into(),
            version: 1,
        };
        let mut registry = SchemaRegistry::new();
        registry.register(
            extension.name.clone(),
            extension.version,
            json!({"type":"object"}),
            [1; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}},"additionalProperties":false})),
        )?;
        let scope = issuer.issue("owner", &Capabilities::new(["extension:configure", "extension:activate"]), None, None, &mut None).expect("scope proof");
        let mut reducer = Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), registry.clone());
        let select = |operation_id, key, revision| -> Result<Command> {
            Ok(Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(key)?,
                expected_revision: revision,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::SelectExtensions {
                    roots: vec![extension.clone()],
                },
            })
        };
        assert!(matches!(
            reducer.apply(select(operation(71), "before-configuration", 0)?),
            Err(Error::Conflict(_))
        ));
        let first = request_file(br#"{"enabled":true}"#)?;
        let invalid = request_file(br#"{"enabled":false,"extra":1}"#)?;
        assert!(
            reducer
                .validate_configuration_bytes(
                    &extension,
                    &invalid,
                    br#"{"enabled":false,"extra":1}"#, None
                )
                .is_err()
        );
        reducer.validate_configuration_bytes(&extension, &first, br#"{"enabled":true}"#, None)?;
        reducer.apply(Command {
            operation_id: operation(72),
            idempotency_key: IdempotencyKey::new("configure-first")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::ConfigureExtension {
                extension: extension.clone(),
                content: first.clone(),
            },
        })?;
        reducer.apply(select(operation(73), "activate-first", 1)?)?;
        assert_eq!(reducer.active_configurations()[0].content, first);
        let Some(admitted) = reducer.extension_admission()? else {
            return Err(Error::NotFound("committed selection".into()));
        };
        assert_eq!(admitted.source().revision, 2);
        assert_eq!(admitted.configurations()[0].content, first);
        let runtime_scope = crate::runtime::RuntimeScope::new(
            Capabilities::new(["extension:activate"]),
            crate::conversation::Limits::default(),
        )?
        .with_extensions_from(&reducer)?;
        assert_eq!(runtime_scope.extensions(), Some(&admitted));
        assert!(
            runtime_scope
                .narrow(
                    Capabilities::new(std::iter::empty::<&str>()),
                    crate::conversation::Limits::default(),
                )?
                .with_extensions_from(&reducer)
                .is_err()
        );
        let second = request_file(br#"{"enabled":false}"#)?;
        reducer.validate_configuration_bytes(&extension, &second, br#"{"enabled":false}"#, None)?;
        reducer.apply(Command {
            operation_id: operation(74),
            idempotency_key: IdempotencyKey::new("configure-second")?,
            expected_revision: 2,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::ConfigureExtension {
                extension: extension.clone(),
                content: second,
            },
        })?;
        reducer.set_resident_event_limit(1)?;
        assert_eq!(reducer.snapshot(usize::MAX, &mut None)?.0.events.len(), 1);
        assert_eq!(reducer.archived_through_revision(), 2);
        assert_eq!(reducer.active_configurations()[0].content, first);
        assert_eq!(reducer.extension_admission()?, Some(admitted));
        let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None)?.0)?;
        let snapshot = crate::contract::json_from_slice(&wire)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let restored = Reducer::restore(snapshot, issuer.verifier(&mut None).expect("verifier copy"), registry)?;
        assert_eq!(
            restored.active_configurations(),
            reducer.active_configurations()
        );
        assert_eq!(
            restored.extension_admission()?,
            reducer.extension_admission()?
        );
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_snapshot_round_trip(&reducer, &issuer, 1)?;
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
            "example../private",
            "example.state/path",
            "example.state\\path",
            "example.state:other",
        ] {
            assert!(validate_extension_name(invalid).is_err());
            assert!(
                SchemaRegistry::new()
                    .register(
                        invalid,
                        1,
                        json!({"type":"object"}),
                        [1; 32],
                        ExtensionForkPolicy::Inherit, [], None
                    )
                    .is_err()
            );
        }
        assert!(validate_extension_name(&format!("example.{}", "x".repeat(256))).is_ok());
        validate_extension_name("example.state")?;
        Ok(())
    }

    #[test]
    fn extension_activation_requires_exact_acyclic_dependencies() -> Result<()> {
        let alpha = ExtensionDependency {
            name: "example.alpha".into(),
            version: 1,
        };
        let beta = ExtensionDependency {
            name: "example.beta".into(),
            version: 2,
        };
        let mut registry = SchemaRegistry::new();
        registry.register(
            alpha.name.clone(),
            alpha.version,
            json!({"type":"object"}),
            [1; 32],
            ExtensionForkPolicy::Inherit,
            [beta.clone()], None,
        )?;
        assert!(matches!(
            registry.activation_order(&[alpha.clone()], &mut None),
            Err(Error::Unsupported(_))
        ));
        registry.register(
            beta.name.clone(),
            beta.version,
            json!({"type":"object"}),
            [2; 32],
            ExtensionForkPolicy::Reset,
            [], None,
        )?;
        assert_eq!(
            registry.activation_order(&[alpha.clone()], &mut None)?.0,
            vec![beta.clone(), alpha.clone()]
        );
        let other_beta = ExtensionDependency {
            name: beta.name.clone(),
            version: 3,
        };
        registry.register(
            other_beta.name.clone(),
            other_beta.version,
            json!({"type":"object"}),
            [3; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        assert!(matches!(
            registry.activation_order(&[alpha.clone(), other_beta], &mut None),
            Err(Error::Conflict(_))
        ));
        let mut cyclic = SchemaRegistry::new();
        cyclic.register(
            alpha.name.clone(),
            alpha.version,
            json!({"type":"object"}),
            [1; 32],
            ExtensionForkPolicy::Inherit,
            [beta.clone()], None,
        )?;
        cyclic.register(
            beta.name.clone(),
            beta.version,
            json!({"type":"object"}),
            [2; 32],
            ExtensionForkPolicy::Reset,
            [alpha], None,
        )?;
        assert!(matches!(
            cyclic.activation_order(&[beta], &mut None),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn agent_extension_selection_replays_and_does_not_retarget_old_events() -> Result<()> {
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "agent-extension-selection".into(),
        };
        let issuer = AuthorityIssuer::new("extension-host", [44; 32], authority.clone());
        let mut registry = SchemaRegistry::new();
        let base = ExtensionDependency {
            name: "example.base".into(),
            version: 1,
        };
        let feature = ExtensionDependency {
            name: "example.feature".into(),
            version: 2,
        };
        registry.register(
            base.name.clone(),
            base.version,
            json!({"type":"object"}),
            [1; 32],
            ExtensionForkPolicy::Inherit, [], None,
        )?;
        registry.register(
            feature.name.clone(),
            feature.version,
            json!({"type":"object"}),
            [2; 32],
            ExtensionForkPolicy::Reset,
            [base.clone()], None,
        )?;
        let scope = issuer.issue("owner", &Capabilities::new(["extension:activate"]), None, None, &mut None).expect("scope proof");
        let select = Command {
            operation_id: operation(61),
            idempotency_key: IdempotencyKey::new("select-extensions")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::SelectExtensions {
                roots: vec![feature.clone()],
            },
        };
        let mut reducer = Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), registry.clone());
        assert!(matches!(
            reducer.apply(select.clone())?,
            ApplyResult::Applied { .. }
        ));
        assert_eq!(reducer.active_extensions(), &[base, feature]);
        let mut restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, issuer.verifier(&mut None).expect("verifier copy"), registry)?;
        assert_eq!(restored.active_extensions(), reducer.active_extensions());
        assert!(matches!(
            restored.apply(select.clone())?,
            ApplyResult::Replayed { .. }
        ));
        restored.apply(Command {
            operation_id: operation(62),
            idempotency_key: IdempotencyKey::new("disable-extensions")?,
            expected_revision: 1,
            scope,
            causal_parent: None,
            action: Action::SelectExtensions { roots: Vec::new() },
        })?;
        assert!(restored.active_extensions().is_empty());
        assert!(matches!(
            restored.apply(select)?,
            ApplyResult::Replayed { .. }
        ));
        assert!(restored.active_extensions().is_empty());
        let active_scope = issuer.issue("owner-active", &Capabilities::new(["extension:activate", "lifecycle:manage"]), None, None, &mut None).expect("scope proof");
        restored.apply(Command {
            operation_id: operation(63),
            idempotency_key: IdempotencyKey::new("activate-agent")?,
            expected_revision: 2,
            scope: active_scope.clone(),
            causal_parent: None,
            action: Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            },
        })?;
        assert!(matches!(
            restored.apply(Command {
                operation_id: operation(64),
                idempotency_key: IdempotencyKey::new("unsafe-extension-switch")?,
                expected_revision: 3,
                scope: active_scope,
                causal_parent: None,
                action: Action::SelectExtensions { roots: Vec::new() },
            }),
            Err(Error::Conflict(_))
        ));
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
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "effects",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
            )?,
            "requests/request.json",
            "generation-1",
            FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
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
            FileDescriptor::from_bytes(&bytes, "application/schema+json", &mut None)?,
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
                    ExtensionForkPolicy::Inherit, [], None,
                )
                .is_ok()
        );
        schemas
    }
    fn reducer_fixture() -> Reducer {
        let owner = issuer();
        Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), schemas())
    }

    fn command(operation_id: OperationId, expected_revision: u64, action: Action) -> Command {
        let capabilities = Capabilities::new([
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
        ]);
        let mut capabilities: Vec<String> = capabilities.iter().map(str::to_owned).collect();
        if let Action::PublishFork { seed } = &action {
            for resource in &seed.resources {
                if let crate::fork::ResourceRevision::PrivateVolume { volume, .. }
                    | crate::fork::ResourceRevision::Project { volume, .. } = &resource.source
                {
                    capabilities.push(
                        volume
                            .capability(crate::conversation::VolumeOperation::Read, &mut None)
                            .expect("valid source reference"),
                    );
                }
            }
        }
        let capabilities = Capabilities::new(capabilities);
        let scope = if matches!(&action, Action::PublishFork { .. }) {
            issuer().issue("root", &capabilities, Some(crate::AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof")
        } else {
            issuer().issue("root", &capabilities, None, None, &mut None).expect("scope proof")
        };
        Command {
            operation_id,
            idempotency_key: IdempotencyKey(format!("key-{operation_id}")),
            expected_revision,
            scope,
            causal_parent: None,
            action,
        }
    }

    #[test]
    fn committed_events_attest_admission_without_persisting_bearer_scopes() -> Result<()> {
        let verifier_debug = format!("{:?}", issuer().verifier(&mut None).expect("verifier copy"));
        assert!(verifier_debug.contains("[redacted]"));
        assert!(!verifier_debug.contains("[7, 7, 7"));
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let mut reducer = Reducer::new(authority, issuer().verifier(&mut None).expect("verifier copy"), schemas());
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
            reducer.apply_committed(tampered, &mut None),
            Err(Error::Unauthorized(_))
        ));
        reducer.apply_committed(event, &mut None)?;
        assert_eq!(reducer.revision(), 1);
        Ok(())
    }

    #[test]
    fn interactions_are_ref_only_versioned_and_responder_bound() -> Result<()> {
        use crate::interaction::{Interaction, InteractionKind, InteractionOutcome};
        let mut reducer = reducer_fixture();
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
                        operation_id: operation(24),
                    },
                    detail: None,
                },
            },
        );
        unknown.scope = issuer().issue("responder", &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]), None, None, &mut None).expect("scope proof");
        let mut misattributed = unknown.clone();
        if let Action::ResolveInteraction { resolution } = &mut misattributed.action {
            resolution.outcome = InteractionOutcome::Indeterminate {
                operation_id: operation(30),
            };
        }
        assert!(matches!(
            reducer.plan(&misattributed),
            Err(Error::Conflict(_))
        ));
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
        granted.scope = issuer().issue("responder", &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]), None, None, &mut None).expect("scope proof");
        let committed = crate::interaction::ResolutionReceipt::from_apply_result(
            reducer.apply(granted.clone())?,
        )?;
        assert_eq!(committed.id, ticket.id);
        assert_eq!(committed.version, 2);
        assert_eq!(committed.conversation_revision, 4);
        assert!(!committed.replayed);
        assert_eq!(reducer.operation_revision(operation(21)), Some(4));
        let replayed =
            crate::interaction::ResolutionReceipt::from_apply_result(reducer.apply(granted)?)?;
        assert!(replayed.replayed);
        assert_eq!(
            replayed.conversation_revision,
            committed.conversation_revision
        );
        assert_eq!(
            reducer
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref()),
            Some(&resolution)
        );
        let mut stale = command(operation(22), 4, Action::ResolveInteraction { resolution });
        stale.scope = issuer().issue("responder", &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]), None, None, &mut None).expect("scope proof");
        assert!(matches!(reducer.plan(&stale), Err(Error::Conflict(_))));
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        let restored = Reducer::restore(snapshot, issuer().verifier(&mut None).expect("verifier copy"), schemas())?;
        assert!(
            restored
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref())
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn extension_state_version_change_requires_exact_migration_event() -> Result<()> {
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let mut registry = schemas();
        registry.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        let mut reducer = Reducer::new(authority.clone(), issuer().verifier(&mut None).expect("verifier copy"), registry.clone());
        reducer.apply(command(
            operation(110),
            0,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(br#"{"text":"old"}"#)?,
            },
        ))?;
        let previous = EventReference {
            authority,
            revision: 1,
        };
        assert!(matches!(
            reducer.apply(command(
                operation(111),
                1,
                Action::AppendCustom {
                    schema: "example.message".into(),
                    version: 2,
                    content: request_file(br#"{"text":"new"}"#)?,
                },
            )),
            Err(Error::Conflict(_))
        ));
        let mut migrate = command(
            operation(112),
            1,
            Action::MigrateExtensionState {
                name: "example.message".into(),
                previous,
                to_version: 2,
                content: request_file(br#"{"text":"new"}"#)?,
            },
        );
        migrate.scope = issuer().issue("owner", &Capabilities::new([
                "extension:migrate".to_owned(),
                request_file(b"null")?
                    .volume()
                    .capability(VolumeOperation::Write, &mut None)?,
            ]), Some(crate::AgentId::from_bytes([8; 16])), None, &mut None).expect("scope proof");
        let mut stale = migrate.clone();
        if let Action::MigrateExtensionState { previous, .. } = &mut stale.action {
            previous.revision = 0;
        }
        assert!(matches!(
            reducer.plan_with_migration_boundary_with_work(&stale, true, false, None, &mut None).map(|(result, _)| result),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            reducer.apply(migrate.clone()),
            Err(Error::Unsupported(_))
        ));
        let ApplyResult::Applied { event } = reducer.plan_with_migration_boundary_with_work(&migrate, true, false, None, &mut None).map(|(result, _)| result)? else {
            return Err(Error::Conflict("migration unexpectedly replayed".into()));
        };
        reducer.apply_committed(event, &mut None)?;
        assert!(matches!(
            reducer.apply(migrate)?,
            ApplyResult::Replayed { .. }
        ));
        assert_eq!(
            reducer
                .extension_state("example.message")
                .map(|(_, state)| state.version),
            Some(2)
        );
        let restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, issuer().verifier(&mut None).expect("verifier copy"), registry)?;
        assert_eq!(
            restored.extension_state("example.message"),
            reducer.extension_state("example.message")
        );
        assert_eq!(restored.events_after(0, 10)?.len(), 2);
        Ok(())
    }

    #[test]
    fn extension_migration_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/extension-state-migration.json").trim();
        let migration: ExtensionStateMigration =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        migration.validate()?;
        let actual =
            serde_json::to_string(&migration).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(actual, fixture);
        Ok(())
    }

    #[test]
    fn extension_replay_requires_exact_implementation_and_fork_policy() -> Result<()> {
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let mut original = Reducer::new(authority.clone(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
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
            bindings.register("example.message", 1, schema.clone(), digest, policy, [], None)?;
            let mut restored = Reducer::new(authority.clone(), issuer().verifier(&mut None).expect("verifier copy"), bindings);
            assert!(matches!(
                restored.apply_committed(event.clone(), &mut None),
                Err(Error::Conflict(_))
            ));
        }
        let mut missing = Reducer::new(
            authority.clone(),
            issuer().verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        );
        assert!(matches!(
            missing.apply_committed(event.clone(), &mut None),
            Err(Error::Unsupported(_))
        ));
        let mut exact = Reducer::new(authority, issuer().verifier(&mut None).expect("verifier copy"), schemas());
        assert!(matches!(
            exact.apply_committed(event, &mut None)?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            exact.schemas.register(
                "example.message",
                1,
                schema,
                [8; 32],
                ExtensionForkPolicy::Inherit, [], None
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
            .verifier(&mut None).expect("verifier copy"),
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
        start.scope = turn_issuer.issue("turn", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof");
        reducer.apply(start)?;
        let mut finish = command(
            operation(41),
            1,
            Action::TransitionLifecycle {
                to: LifecycleState::Completed,
                reason: None,
            },
        );
        finish.scope = turn_issuer.issue("turn", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof");
        reducer.apply(finish)?;
        let mut reopen = command(
            operation(42),
            2,
            Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            },
        );
        reopen.scope = turn_issuer.issue("turn", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof");
        assert!(matches!(reducer.apply(reopen), Err(Error::Conflict(_))));
        Ok(())
    }

    #[test]
    fn effect_guarantees_and_attestations_control_retry() -> Result<()> {
        let effect_id = EffectId::from_bytes([9; 16]);
        let first_attempt = EffectAttemptId::from_bytes([10; 16]);
        let retry_attempt = EffectAttemptId::from_bytes([11; 16]);
        for (guarantee, request, result_schema) in [
            (EffectGuarantee::IdempotentRetry, br#"{"value":1}"#.as_slice(),
                json!({"type": "object", "required": ["receipt"]})),
            (EffectGuarantee::AtMostOnce, b"null".as_slice(), json!({})),
            (EffectGuarantee::ExactlyOnce, br#"{"value":1}"#.as_slice(), json!({"type": "string"})),
        ] {
            let mut reducer = reducer_fixture();
            reducer.apply(command(operation(1), 0, Action::PlanEffect {
                effect_id, provider: "example.provider".into(), guarantee,
                effect_kind: "example.send".into(), request: request_file(request)?,
                result_schema: schema_file(&result_schema)?,
            }))?;
            assert!(!serde_json::to_string(&reducer.snapshot(usize::MAX, &mut None)?.0)
                .map_err(|error| Error::Invalid(error.to_string()))?.contains("\\\"value\\\""));
            reducer.apply(command(operation(2), 1, Action::MarkEffectDispatched {
                effect_id, attempt_id: first_attempt,
            }))?;
            if guarantee == EffectGuarantee::ExactlyOnce {
                let effect = reducer.effect(effect_id).ok_or_else(|| Error::NotFound("effect".into()))?;
                let invalid_result = issuer().attest_effect(effect_id, effect, first_attempt,
                    EffectStatus::Succeeded { result: FileRef::new(
                        request_file(b"1")?.volume().clone(), "results/invalid.txt", "generation-1",
                        crate::conversation::FileDescriptor::from_bytes(b"1", "text/plain", &mut None)?,
                        "invalid.txt", &mut None,
                    )? }, &mut None)?;
                assert!(matches!(reducer.apply(command(operation(32), 2,
                    Action::ResolveEffect { observation: invalid_result })), Err(Error::Invalid(_))));
                let mut forged = issuer().attest_effect(effect_id,
                    reducer.effect(effect_id).ok_or_else(|| Error::NotFound("effect".into()))?, first_attempt,
                    EffectStatus::Indeterminate, &mut None)?;
                forged.provider = "attacker".into();
                assert!(matches!(reducer.apply(command(operation(33), 2,
                    Action::ResolveEffect { observation: forged })), Err(Error::Unauthorized(_))));
            }
            let indeterminate = issuer().attest_effect(effect_id,
                reducer.effect(effect_id).ok_or_else(|| Error::NotFound("effect".into()))?,
                first_attempt, EffectStatus::Indeterminate, &mut None)?;
            reducer.apply(command(operation(3), 2, Action::ResolveEffect { observation: indeterminate }))?;
            assert_eq!(reducer.effect(effect_id).map(|effect| &effect.status), Some(&EffectStatus::Indeterminate));
            let retry = reducer.apply(command(operation(4), 3, Action::MarkEffectDispatched {
                effect_id, attempt_id: retry_attempt,
            }));
            if guarantee == EffectGuarantee::IdempotentRetry {
                retry?;
                let succeeded = issuer().attest_effect(effect_id,
                    reducer.effect(effect_id).ok_or_else(|| Error::NotFound("effect".into()))?,
                    retry_attempt, EffectStatus::Succeeded {
                        result: request_file(br#"{"receipt":"confirmed"}"#)?,
                    }, &mut None)?;
                reducer.apply(command(operation(5), 4, Action::ResolveEffect { observation: succeeded }))?;
                assert!(matches!(reducer.effect(effect_id).map(|effect| &effect.status), Some(EffectStatus::Succeeded { .. })));
                assert_eq!(reducer.effect(effect_id).map(|effect| effect.attempts.len()), Some(2));
            } else {
                assert!(matches!(retry, Err(Error::Conflict(_))));
            }
        }
        Ok(())
    }

    #[test]
    fn malformed_admission_metadata_is_rejected() -> Result<()> {
        let mut reducer = reducer_fixture();
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
    fn fork_is_invisible_until_one_seed_event_commits() -> Result<()> {
        use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
        use crate::fork::{CapturedResource, ResourceRevision};
        use crate::resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef, StreamRef};

        let parent = Authority { kind: AggregateKind::Conversation, id: "conversation-1".into() };
        let child = Authority { kind: AggregateKind::Conversation, id: "conversation-2".into() };
        let child_agent = crate::AgentId::from_bytes([6; 16]);
        let mut seed: ForkSeed = serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        seed.operation_id = operation(8);
        seed.parent = parent.clone();
        seed.parent_revision = 1;
        seed.child = child.clone();
        seed.child_agent = child_agent;
        seed.resources.rotate_right(1);
        let provider = seed.child_private_volume.provider().clone();
        seed.child_private_volume = VolumeRef::new(provider.clone(), "child-private",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent))?;
        if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].source {
            *volume = VolumeRef::new(provider.clone(), "parent-private", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])))?;
        }
        if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].revision {
            *volume = seed.child_private_volume.clone();
        }
        let history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?, parent.stream_path(&mut None)?.into_bytes(),
            Some("1".into()))?);
        seed.resources[1] = CapturedResource { source: history.clone(), revision: history };
        let extension = ResourceRevision::Extension {
            name: "example.message".into(),
            version: 1,
            implementation_digest: [9; 32],
            reference: ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test".into(), "objects".into(), "1".into(), &mut None)?,
                b"extension-state".to_vec().into(),
                Some("immutable-1".into()), &mut None
            )?,
        };
        let mut extension_seed = seed.clone();
        extension_seed.resources.push(CapturedResource {
            source: extension.clone(),
            revision: extension,
        });
        let mut reset_seed = extension_seed.clone();
        if let ResourceRevision::Extension { reference, .. } = &mut reset_seed.resources[3].revision
        {
            *reference = ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test".into(), "objects".into(), "1".into(), &mut None)?,
                b"child-extension-state".to_vec().into(),
                Some("immutable-1".into()), &mut None
            )?;
        }
        for (policy, selected_seed, allowed) in [
            (ExtensionForkPolicy::Inherit, &extension_seed, true),
            (ExtensionForkPolicy::Reset, &extension_seed, false),
            (ExtensionForkPolicy::Reject, &extension_seed, false),
            (ExtensionForkPolicy::Inherit, &reset_seed, false),
            (ExtensionForkPolicy::Reset, &reset_seed, true),
        ] {
            let mut bindings = SchemaRegistry::new();
            bindings.register("example.message", 1, json!({"type": "object"}), [9; 32], policy, [], None)?;
            let mut candidate = Reducer::new(parent.clone(), issuer().verifier(&mut None).expect("verifier copy"), bindings);
            candidate.apply(command(operation(1), 0, Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            }))?;
            let result = candidate.plan(&command(operation(8), 1, Action::PublishFork {
                seed: Box::new((*selected_seed).clone()),
            }));
            assert_eq!(result.is_ok(), allowed, "fork policy {policy:?}: {result:?}");
        }
        let mut mismatched = extension_seed;
        if let ResourceRevision::Extension {
            implementation_digest,
            ..
        } = &mut mismatched.resources[3].revision
        {
            *implementation_digest = [8; 32];
        }
        mismatched.resources[3].source = mismatched.resources[3].revision.clone();
        let mut candidate = Reducer::new(parent.clone(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
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
        let mut reducer = reducer_fixture();
        reducer.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        #[cfg(feature = "filesystem")]
        {
            use acyclic_fs::{CancellationToken, WorkBudget};
            let budget = WorkBudget {
                bytes_copied: 16_777_216, bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
                items_examined: 1_000_000, allocation_operations: 100_000, peak_allocation_bytes: 268_435_456,
                ..WorkBudget::default()
            };
            let cancellation = CancellationToken::new();
            let mut work = crate::store::history::HistoryWork::new(budget, &cancellation);
            reducer.validate_fork_reference_ownership_with_work(&seed, &mut Some(&mut work))?;
            reducer.require_fresh_fork_with_work(&seed, &mut Some(&mut work))?;
            let receipt = work.finish(Ok(())).map_err(|failure| failure.error)?;
            assert!(receipt.work.items_examined > 0);
            let mut narrow = crate::store::history::HistoryWork::new(
                WorkBudget { items_examined: 1, ..budget }, &cancellation);
            assert!(reducer.require_fresh_fork_with_work(&seed, &mut Some(&mut narrow)).is_err());
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let mut work = crate::store::history::HistoryWork::new(budget, &cancelled);
            assert!(reducer.validate_fork_reference_ownership_with_work(&seed, &mut Some(&mut work)).is_err());

            let prefix = InheritedConversationPrefix::select(parent.clone(), 1, 0, &[], &reducer.conversation, &mut None)?;
            let prefix_bytes = crate::contract::canonical_json_bytes(&prefix)?;
            let mut materialized = seed.clone();
            materialized.inherited_context.push(FileRef::new(
                seed.child_private_volume.clone(), ".system/inherited-conversation/prefix.json", "generation-1",
                FileDescriptor::from_bytes(&prefix_bytes,
                    "application/vnd.acyclic.harness.inherited-conversation+json", &mut None)?,
                "inherited-conversation.json",
            )?);
            reducer.validate_fork_reference_ownership_with_work(&materialized, &mut None)?;
            let mut work = crate::store::history::HistoryWork::new(budget, &cancellation);
            reducer.validate_fork_reference_ownership_with_work(&materialized, &mut Some(&mut work))?;
            let receipt = work.finish(Ok(())).map_err(|failure| failure.error)?;
            assert!(receipt.work.bytes_encoded > 0);
            assert!(receipt.work.bytes_hashed >= prefix_bytes.len() as u64);
            let mut work = crate::store::history::HistoryWork::new(
                WorkBudget { bytes_encoded: 1, ..budget }, &cancellation);
            assert!(reducer.validate_fork_reference_ownership_with_work(&materialized, &mut Some(&mut work)).is_err());
            let mut hostile_manifests = seed.clone();
            hostile_manifests.attachment_manifests.push(request_file(b"[]")?);
            assert!(matches!(reducer.validate_fork_reference_ownership_with_work(&hostile_manifests, &mut None), Err(Error::Invalid(_))));
            let mut work = crate::store::history::HistoryWork::new(budget, &cancellation);
            let result = reducer.validate_fork_reference_ownership_with_work(&hostile_manifests, &mut Some(&mut work));
            let failure = work.finish(result).expect_err("unpublished manifest must fail on the original path");
            assert!(failure.work.bytes_encoded > 0);
            assert!(failure.work.bytes_hashed > 0);
            let mut work = crate::store::history::HistoryWork::new(
                WorkBudget { peak_allocation_bytes: 1, ..budget }, &cancellation);
            assert!(reducer.validate_fork_reference_ownership_with_work(&hostile_manifests, &mut Some(&mut work)).is_err());
        }
        let mut foreign_command = command(
            operation(8),
            1,
            Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        );
        foreign_command.scope = issuer().issue("foreign-agent", &Capabilities::new(["event:append", "fork:publish"]), Some(crate::AgentId::from_bytes([2; 16])), None, &mut None).expect("scope proof");
        assert!(matches!(
            reducer.plan(&foreign_command),
            Err(Error::Unauthorized(_))
        ));
        foreign_command.scope = issuer().issue("agentless-root", &Capabilities::new(["event:append", "fork:publish"]), None, None, &mut None).expect("scope proof");
        assert!(matches!(
            reducer.plan(&foreign_command),
            Err(Error::Unauthorized(_))
        ));
        let mut unreadable_source = command(
            operation(8), 1, Action::PublishFork { seed: Box::new(seed.clone()) },
        );
        let project_read = seed.resources.iter().find_map(|resource| match &resource.source {
            ResourceRevision::Project { volume, .. } => Some(volume.capability(VolumeOperation::Read, &mut None)),
            _ => None,
        }).ok_or_else(|| Error::Invalid("fixture project source is absent".into()))??;
        unreadable_source.scope = issuer().issue("current-agent", &Capabilities::new(unreadable_source.scope.capabilities().iter().filter(|grant| *grant != project_read)), Some(crate::AgentId::from_bytes([1;16])), None, &mut None).expect("scope proof");
        assert!(matches!(reducer.plan(&unreadable_source), Err(Error::Unauthorized(_))));
        assert_eq!(reducer.revision(), 1);
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
        let mut foreign_event = event.clone();
        foreign_event.scope = RecordedScope::from_scope(&issuer().issue("foreign-agent", &Capabilities::new(["event:append", "fork:publish"]), Some(crate::AgentId::from_bytes([2; 16])), None, &mut None).expect("scope proof"));
        foreign_event.attestation = issuer().verifier(&mut None).expect("verifier copy").attest_event(&foreign_event, &mut None)?;
        assert!(matches!(
            reducer.apply_committed(foreign_event, &mut None),
            Err(Error::Unauthorized(_))
        ));
        reducer.apply_committed(event, &mut None)?;
        assert_eq!(reducer.fork(&child), Some(&seed));
        let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None)?.0)?;
        let snapshot = crate::contract::json_from_slice(&wire)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let restored = Reducer::restore(snapshot, issuer().verifier(&mut None).expect("verifier copy"), schemas())?;
        assert_eq!(restored, reducer);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_snapshot_round_trip(&reducer, &issuer(), usize::MAX)?;
        reducer = restored;
        let mut reused_private = seed;
        reused_private.operation_id = operation(9);
        reused_private.child.id = "conversation-3".into();
        reused_private.parent_revision = 2;
        let next_history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?,
            parent.stream_path(&mut None)?.into_bytes(),
            Some("2".into()),
        )?);
        reused_private.resources[1].source = next_history.clone();
        reused_private.resources[1].revision = next_history;
        reused_private.validate(&mut None)?;
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
    fn scope_attenuation_cannot_expand_authority() {
        let issuer = issuer();
        let root = issuer.issue("root", &Capabilities::new(["read", "write"]), None, None, &mut None).expect("scope proof");
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
        let scope = trusted.issue("root", &Capabilities::new(["event:append"]), None, None, &mut None).expect("scope proof");
        let foreign_authority = Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        };
        let foreign = AuthorityIssuer::new("test", [7; 32], foreign_authority.clone());
        let foreign_reducer = Reducer::new(foreign_authority, foreign.verifier(&mut None).expect("verifier copy"), schemas());
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
            trusted.verifier(&mut None).expect("verifier copy"),
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
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "agent-9",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        let content = FileRef::new(
            volume,
            "messages/one.txt",
            "generation-1",
            FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?,
            "one.txt",
        )?;
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let verifier = issuer().verifier(&mut None).expect("verifier copy");
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
                        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
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
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
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
            checkpoint: None,
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
        assert_eq!(
            reducer.resident_context_selections().collect::<Vec<_>>(),
            vec![&selection]
        );
        assert!(matches!(
            reducer.plan(&command(
                operation(4),
                3,
                Action::SelectModelContext {
                    selection: ModelContextSelection {
                        checkpoint: None,
                        conversation_revision: 0,
                        message_ids: vec![message.id]
                    },
                }
            )),
            Err(Error::Conflict(_))
        ));
        let restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, verifier.clone(), schemas())?;
        assert_eq!(restored.conversation(), reducer.conversation());
        assert_eq!(
            restored.resident_context_selections().collect::<Vec<_>>(),
            reducer.resident_context_selections().collect::<Vec<_>>()
        );
        let mut canonical = restored;
        reducer.set_resident_event_limit(1)?;
        for identity in 5..=68 {
            let next = command(
                operation(identity),
                reducer.revision(),
                Action::SelectModelContext {
                    selection: selection.clone(),
                },
            );
            assert!(matches!(reducer.plan(&next), Err(Error::Unsupported(_))));
            let ApplyResult::Applied { event } = canonical.apply(next)? else {
                return Err(Error::Invalid(
                    "fresh canonical selection was not admitted".into(),
                ));
            };
            reducer.apply_committed(event, &mut None)?;
            assert_eq!(reducer.resident_context_selections().count(), 1);
        }
        assert!(
            reducer
                .context_selection_for_operation(operation(3))
                .is_none()
        );
        assert_eq!(
            reducer.context_selection_for_operation(operation(68)),
            Some(&selection)
        );
        let restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, verifier, schemas())?;
        assert_eq!(restored.resident_context_selections().count(), 1);
        assert_eq!(
            restored.context_selection_for_operation(operation(68)),
            Some(&selection)
        );
        Ok(())
    }
}
