//! authority semantics.
use super::*;
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
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.id.is_empty()
            || AUTHORITY_ID_FORBIDDEN_EXACT.contains(&self.id.as_str())
            || self.id.contains(AUTHORITY_ID_FORBIDDEN_SEPARATORS)
            || self.id.chars().any(char::is_control)
        {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "aggregate identity is not a safe path segment",
            ));
        }
        Ok(())
    }

    /// Returns the Stream path for this aggregate.
    pub fn stream_path(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        self.validate_with_work(work)?;
        let segment = match self.kind {
            AggregateKind::Agent => "agents",
            AggregateKind::Conversation => "conversations",
            AggregateKind::Session => "sessions",
            AggregateKind::Turn => "turns",
            AggregateKind::Task => "tasks",
        };
        let bytes = "harness/v2/"
            .len()
            .checked_add(segment.len())
            .and_then(|bytes| bytes.checked_add(1))
            .and_then(|bytes| bytes.checked_add(self.id.len()))
            .ok_or_else(|| Error::Invalid("aggregate path length overflows".into()))?;
        let mut path = String::new();
        if let Some(work) = work.as_deref_mut() {
            work.reserve_text(&mut path, bytes)?;
            work.copy_bytes(bytes as u64)?;
        } else {
            path.reserve(bytes);
        }
        path.push_str("harness/v2/");
        path.push_str(segment);
        path.push('/');
        path.push_str(&self.id);
        Ok(path)
    }
}

#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
/// Immutable authorization scope captured at admission.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Stable scope identity.
    pub(in crate::core) id: String,
    /// Effective capabilities after policy intersection.
    pub(in crate::core) capabilities: Capabilities,
    /// Host-attested agent on whose behalf this operation runs, if any.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub(in crate::core) agent: Option<AgentId>,
    /// Trusted issuer identity.
    pub(in crate::core) issuer: String,
    /// Parent proof, present for attenuated scopes.
    #[cfg_attr(feature = "wasm", tsify(type = "number[] | null"))]
    pub(in crate::core) parent_proof: Option<[u8; 32]>,
    /// Keyed proof over the complete grant.
    pub(in crate::core) proof: [u8; 32],
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

    /// Copies the immutable grant under the caller's optional ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            copy_owned(self, quota, |scope, _| scope.logical_owned_bytes())
        })
        .map(|(scope, _)| scope)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let capabilities = self.capabilities.logical_owned_bytes()?;
        (self.id.capacity() as u64)
            .checked_add(self.issuer.capacity() as u64)
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
    pub(in crate::core) id: String,
    pub(in crate::core) key: [u8; 32],
    pub(in crate::core) audience: Authority,
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
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() {
            return Ok(self.clone());
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let cloned = Self {
            id: copying::copy_text(&self.id, &mut quota)?,
            key: self.key,
            audience: copying::copy_authority(&self.audience, &mut quota)?,
        };
        quota.transfer();
        Ok(cloned)
    }
    /// Borrows the original configured issuer identity without exposing its key.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn audience(&self) -> &Authority {
        &self.audience
    }

    /// Copies the configured verifier using the caller's ledger.
    pub fn verifier(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        let Self { id, key, audience } = self.copy(work)?;
        Ok(AuthorityVerifier { id, key, audience })
    }

    /// Signs one grant using the caller's optional ledger.
    pub fn issue(
        &self,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Scope> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let id = copying::copy_text(id, &mut quota)?;
        let capabilities = copying::copy_capabilities(capabilities, &mut quota)?;
        let issuer = copying::copy_text(&self.id, &mut quota)?;
        let mut scope = Scope {
            id,
            capabilities,
            issuer,
            agent,
            parent_proof,
            proof: [0; 32],
        };
        scope.proof = scope_proof(&self.key, &self.audience, &scope, quota.work())?;
        quota.transfer();
        Ok(scope)
    }

    /// Issues a child grant that cannot widen its parent.
    pub fn attenuate(
        &self,
        parent: &Scope,
        id: impl Into<String>,
        capabilities: Capabilities,
    ) -> Result<Scope> {
        self.verifier(&mut None)
            .expect("verifier copy")
            .verify(parent, &mut None)?;
        if !capabilities.is_subset_of(&parent.capabilities) {
            return Err(Error::Unauthorized(
                "child scope cannot widen ancestor capabilities".into(),
            ));
        }
        self.issue(
            &id.into(),
            &capabilities,
            parent.agent,
            Some(parent.proof),
            &mut None,
        )
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
            &id.into(),
            &Capabilities::new([file.read_capability()?]),
            Some(reader),
            Some(owner_scope.proof),
            &mut None,
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
            &id.into(),
            &Capabilities::new([volume.directory_read_capability(prefix, &mut None)?]),
            Some(reader),
            Some(owner_scope.proof),
            &mut None,
        )
    }

    pub(in crate::core) fn require_private_owner_read(
        &self,
        owner_scope: &Scope,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<()> {
        self.verifier(&mut None)
            .expect("verifier copy")
            .verify(owner_scope, &mut None)?;
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
        let mut observation = EffectAttestation {
            provider: copying::copy_text(&effect.provider, &mut custody)?,
            effect_id,
            attempt_id,
            request_digest: effect.request_digest,
            guarantee: effect.guarantee,
            result_schema_digest: effect.result_schema_digest,
            status,
            proof: [0; 32],
        };
        observation.proof =
            effect_attestation_proof(&self.key, &self.audience, &observation, custody.work())?;
        custody.transfer();
        Ok(observation)
    }
}

/// Reducer-side verifier for host-issued scopes.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthorityVerifier {
    pub(in crate::core) id: String,
    pub(in crate::core) key: [u8; 32],
    pub(in crate::core) audience: Authority,
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
        (self.id.capacity() as u64)
            .checked_add(self.audience.id.capacity() as u64)
            .ok_or_else(|| Error::Invalid("verifier owned byte count overflows".into()))
    }

    /// Copies the verifier using the caller's ledger.
    pub fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
        &self,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.verify_scope_for_with_work(&self.audience, scope, work)
    }

    /// Rebinds only the authenticated audience, retaining this original key.
    pub(crate) fn verify_scope_for_with_work(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let expected = scope_proof(&self.key, audience, scope, work)?;
        if scope.issuer != self.id || scope.proof != expected {
            return Err(Error::Unauthorized("scope proof is invalid".into()));
        }
        Ok(())
    }

    pub(in crate::core) fn canonical_proof<T: Serialize>(
        &self,
        domain: &[u8],
        value: &T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            value,
            crate::store::default_projection_read_limits().maximum_bytes,
            quota.work(),
        )?;
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
        quota.relinquish(quota.retained())?;
        Ok(proof)
    }

    pub(in crate::core) fn attest_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.canonical_proof(
            b"harness/v2/committed-event\0",
            &(
                &self.audience,
                event.revision,
                event.operation_id,
                event.intent_digest,
                &event.scope,
                &event.causal_parent,
                &event.payload,
            ),
            work,
        )
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
        &self,
        revision: u64,
        canonical_digest: [u8; 32],
        root: Option<[u8; 32]>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let path = self.audience.stream_path(quota.work())?;
        quota.adopt(path.capacity() as u64)?;
        let proof = self.canonical_proof(
            b"harness/v1/effect-history-root\0",
            &(
                &self.id,
                &self.audience,
                &path,
                revision,
                canonical_digest,
                root,
            ),
            quota.work(),
        );
        drop(path);
        proof
    }

    pub(crate) fn fork_history_proof(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.verify_audience(&seed.parent, work)?;
        self.canonical_proof(
            b"harness/v1/fork-history-witness\0",
            &(
                (
                    &self.id,
                    &seed.operation_id,
                    &seed.parent,
                    seed.parent_revision,
                    &seed.child,
                    seed.child_agent,
                    &seed.attached_agents,
                    &seed.resources,
                    &seed.omissions,
                    &seed.child_private_volume,
                ),
                (
                    &seed.child_private_generation,
                    &seed.inherited_context,
                    &seed.summary,
                    seed.inherited_through_sequence,
                    &seed.shared_grants,
                    &seed.reference_grants,
                    &seed.attachment_manifests,
                    &seed.boundary,
                ),
            ),
            work,
        )
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn fork_history_selection_proof_with_work(
        &self,
        revision: u64,
        through_sequence: u64,
        agent: Option<AgentId>,
        history_digest: [u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.canonical_proof(
            b"harness/v1/fork-history-selection\0",
            &(
                &self.id,
                &self.audience,
                revision,
                through_sequence,
                agent,
                history_digest,
            ),
            work,
        )
    }

    /// Verifies the configured aggregate audience.
    pub fn verify_audience(
        &self,
        authority: &Authority,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        if &self.audience == authority {
            return Ok(());
        }
        Err(crate::fork::boundary_error(
            work,
            crate::wire::ErrorCode::Unauthorized,
            "authority verifier belongs to another aggregate",
        ))
    }

    pub(in crate::core) fn verify_effect(
        &self,
        observation: &EffectAttestation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let expected = effect_attestation_proof(&self.key, &self.audience, observation, work)?;
        if observation.proof != expected {
            return Err(Error::Unauthorized(
                "effect observation attestation is invalid".into(),
            ));
        }
        Ok(())
    }
}

fn scope_proof(
    key: &[u8; 32],
    audience: &Authority,
    scope: &Scope,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let Scope {
        id,
        capabilities,
        issuer,
        agent,
        parent_proof,
        ..
    } = scope;
    let strings = || {
        [audience.id.as_str(), issuer.as_str(), id.as_str()]
            .into_iter()
            .chain(capabilities.iter())
    };
    if let Some(work) = work.as_deref_mut() {
        let fixed = 3 + u64::from(agent.is_some()) * 16 + u64::from(parent_proof.is_some()) * 32;
        let bytes = strings().try_fold(fixed, |total, text| {
            total
                .checked_add(8)
                .and_then(|total| total.checked_add(text.len() as u64))
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
    for value in strings() {
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(&[u8::from(agent.is_some())]);
    if let Some(agent) = agent {
        hasher.update(&agent.into_bytes());
    }
    hasher.update(&[u8::from(parent_proof.is_some())]);
    if let Some(parent) = parent_proof {
        hasher.update(parent);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn effect_attestation_proof(
    key: &[u8; 32],
    audience: &Authority,
    observation: &EffectAttestation,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let bytes = crate::store::history::canonical_bytes_with_work(
        &(
            audience,
            &observation.provider,
            observation.effect_id,
            observation.attempt_id,
            observation.request_digest,
            observation.guarantee,
            observation.result_schema_digest,
            &observation.status,
        ),
        crate::store::default_projection_read_limits().maximum_bytes,
        custody.work(),
    )?;
    custody.adopt(bytes.capacity() as u64)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(bytes.len() as u64)?;
    }
    let proof = *blake3::keyed_hash(key, &bytes).as_bytes();
    drop(bytes);
    Ok(proof)
}
