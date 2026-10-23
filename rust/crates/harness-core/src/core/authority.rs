//! Aggregate identity and host-issued authorization.
use super::*;
mod runtime_signing;
mod workload;
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
    pub(crate) fn validate_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
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
    pub fn stream_path(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<String> {
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

/// Immutable authorization scope captured at admission.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
    /// Public identity of the authenticated workload tip.
    pub(in crate::core) proof: Box<[u8; 32]>,
    /// Ed25519 workload chain, including only its restricted append tip.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    #[serde(with = "super::capability_bytes")]
    pub(in crate::core) workload: Vec<u8>,
}

impl Drop for Scope {
    fn drop(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.workload);
    }
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

    /// Authenticated service identity that signed this grant.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Verifies the workload using only independently authenticated public trust.
    /// The owner supplies its current revocation snapshot and trusted UTC time.
    pub fn verify_workload(
        &self,
        trust: &dyn acyclic_auth::verify::Trust,
        audience: &Authority,
        now_unix_seconds: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        workload::verify_trusted(trust, audience, self, now_unix_seconds, work).map(|_| ())
    }

    /// Attenuates this workload offline; possession of a runtime root is unnecessary.
    pub fn attenuate_workload(
        &self,
        id: impl Into<String>,
        capabilities: &Capabilities,
        trust: &dyn acyclic_auth::verify::Trust,
        audience: &Authority,
        now_unix_seconds: u64,
    ) -> Result<Self> {
        if !capabilities.is_subset_of(&self.capabilities) {
            return Err(Error::Unauthorized(
                "child scope cannot widen ancestor capabilities".into(),
            ));
        }
        let capability =
            workload::verify_trusted(trust, audience, self, now_unix_seconds, &mut None)?;
        let mut scope = Self {
            id: id.into(),
            capabilities: capabilities.clone(),
            agent: self.agent,
            issuer: self.issuer.clone(),
            parent_proof: Some(*self.proof),
            proof: Box::new([0; 32]),
            workload: Vec::new(),
        };
        workload::append(&capability, &mut scope, None)?;
        Ok(scope)
    }

    /// Narrows expiry offline; the verifier still intersects every ancestor deadline.
    pub fn with_workload_expiry(
        mut self,
        expires_at_unix_seconds: u64,
        trust: &dyn acyclic_auth::verify::Trust,
        audience: &Authority,
        now_unix_seconds: u64,
    ) -> Result<Self> {
        if expires_at_unix_seconds <= now_unix_seconds {
            return Err(Error::Unauthorized("workload deadline has elapsed".into()));
        }
        let capability =
            workload::verify_trusted(trust, audience, &self, now_unix_seconds, &mut None)?;
        self.parent_proof = Some(*self.proof);
        workload::append(&capability, &mut self, Some(expires_at_unix_seconds))?;
        Ok(self)
    }

    /// Copies the immutable grant under the caller's optional ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        CodecQuota::produce(work, |quota| {
            copy(self, quota, |scope, _| scope.logical_owned_bytes())
        })
        .map(|(scope, _)| scope)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let capabilities = self.capabilities.logical_owned_bytes()?;
        (self.id.capacity() as u64)
            .checked_add(self.issuer.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(self.workload.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(32))
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
        workload: Vec<u8>,
    ) -> Self {
        Self {
            id,
            capabilities,
            issuer,
            agent,
            parent_proof,
            proof: Box::new(proof),
            workload,
        }
    }
}

/// Explicit host authority capable of issuing unforgeable scopes.
#[derive(Clone)]
pub struct AuthorityIssuer {
    pub(in crate::core) id: String,
    runtime: std::sync::Arc<runtime_signing::RuntimeSigner>,
    pub(in crate::core) audience: Authority,
}

impl AuthorityIssuer {
    /// Creates an issuer from host-managed key material.
    #[must_use]
    pub fn new(id: impl Into<String>, key: [u8; 32], audience: Authority) -> Self {
        let id = id.into();
        Self {
            runtime: std::sync::Arc::new(runtime_signing::RuntimeSigner::new(&id, key)),
            id,
            audience,
        }
    }

    /// Copies the original issuer metadata under the caller's original ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        if work.is_none() {
            return Ok(self.clone());
        }
        let mut quota = CodecQuota::new(work, 0)?;
        let cloned = Self {
            id: copying::copy_text(&self.id, &mut quota)?,
            runtime: self.runtime.clone(),
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

    pub(crate) const fn audience(&self) -> &Authority {
        &self.audience
    }

    /// Copies the configured verifier using the caller's ledger.
    pub fn verifier(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<AuthorityVerifier> {
        let mut quota = CodecQuota::new(work, 0)?;
        let id = copying::copy_text(&self.id, &mut quota)?;
        let audience = copying::copy_authority(&self.audience, &mut quota)?;
        let verifier = AuthorityVerifier {
            id,
            audience,
            public: self.runtime.public.clone(),
            admission: runtime_signing::AdmissionPort::new(&self.runtime),
        };
        quota.transfer();
        Ok(verifier)
    }

    /// Public local account anchors. Callers must authenticate keyring updates
    /// independently before using a changed snapshot for workload verification.
    pub fn public_workload_trust(&self) -> Result<acyclic_auth::trust::Snapshot> {
        self.runtime
            .public
            .trust
            .read()
            .map(|trust| trust.clone())
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))
    }

    /// Revokes this workload and every descendant at this runtime's public trust boundary.
    pub fn revoke_workload(&self, scope: &Scope) -> Result<()> {
        self.verifier(&mut None)?.verify(scope, &mut None)?;
        self.runtime
            .public
            .trust
            .write()
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))?
            .revoked_block_ids
            .insert(*scope.proof);
        Ok(())
    }

    /// Signs one grant using the caller's optional ledger.
    pub fn issue(
        &self,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Scope> {
        self.issue_bound(id, capabilities, agent, parent_proof, None, work)
    }

    /// Runtime-only delegation across an attested created resource or agent boundary.
    pub(crate) fn issue_derived(
        &self,
        parent: &Scope,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Scope> {
        let capability = workload::verify(
            &self.verifier(work)?,
            &self.audience,
            parent,
            workload::now(),
            work,
        )?;
        self.issue_bound(
            id,
            capabilities,
            agent,
            Some(*parent.proof),
            Some(&capability),
            work,
        )
    }

    fn issue_bound(
        &self,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        parent_proof: Option<[u8; 32]>,
        parent: Option<&acyclic_auth::verify::VerifiedCapability>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Scope> {
        let mut quota = CodecQuota::new(work, 0)?;
        let id = copying::copy_text(id, &mut quota)?;
        let capabilities = copy(capabilities, &mut quota, |value: &Capabilities, _| {
            value.logical_owned_bytes()
        })?;
        let issuer = copying::copy_text(&self.id, &mut quota)?;
        quota.replace(ownership::owned_sum([quota.retained(), 32])?)?;
        let mut scope = Scope {
            id,
            capabilities,
            issuer,
            agent,
            parent_proof,
            proof: Box::new([0; 32]),
            workload: Vec::new(),
        };
        workload::issue(self, &mut scope, parent, quota.work())?;
        quota.adopt(scope.workload.capacity() as u64)?;
        quota.transfer();
        Ok(scope)
    }

    /// Issues a child grant that cannot widen its parent.
    pub fn attenuate(
        &self,
        parent: &Scope,
        id: impl Into<String>,
        capabilities: &Capabilities,
    ) -> Result<Scope> {
        parent.attenuate_workload(
            id,
            capabilities,
            &self.public_workload_trust()?,
            &self.audience,
            workload::now(),
        )
    }

    /// Delegates one pinned private file to another agent without a fork or
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
        self.issue_derived(
            owner_scope,
            &id.into(),
            &Capabilities::new([file.read_capability(&mut None)?]),
            Some(reader),
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
        self.issue_derived(
            owner_scope,
            &id.into(),
            &Capabilities::new([volume.directory_read_capability(prefix, &mut None)?]),
            Some(reader),
            &mut None,
        )
    }

    pub(in crate::core) fn require_private_owner_read(
        &self,
        owner_scope: &Scope,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<()> {
        self.verifier(&mut None)?.verify(owner_scope, &mut None)?;
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

    pub(crate) fn authorize_effect(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<acyclic_auth::verify::VerifiedCapability> {
        self.verifier(work)?
            .verify_effect_plan(effect_id, effect, work)
    }

    /// Signs the original provider observation with admitted encoding and hashing.
    pub(crate) fn attest_effect(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        attempt_id: EffectAttemptId,
        status: EffectStatus,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<EffectAttestation> {
        let parent = self.authorize_effect(effect_id, effect, work)?;
        let mut custody = CodecQuota::new(work, 0)?;
        let mut observation = EffectAttestation {
            provider: copying::copy_text(&effect.provider, &mut custody)?,
            effect_id,
            attempt_id,
            request_digest: effect.request_digest,
            guarantee: effect.guarantee,
            result_schema_digest: effect.result_schema_digest,
            status,
            proof: Vec::new(),
        };
        let scope = self.issue_bound(
            "effect-observation",
            &Capabilities::new([
                "effect:run".to_owned(),
                capability::effect_provider(&effect.provider),
            ]),
            None,
            parent.block_ids().last().copied(),
            Some(&parent),
            custody.work(),
        )?;
        let held = scope.logical_owned_bytes()?;
        custody.adopt(held)?;
        let payload = effect_attestation_digest(&self.audience, &observation, custody.work())?;
        observation.proof = workload::record(
            &self.runtime,
            &self.id,
            &self.audience,
            &scope,
            payload,
            custody.work(),
        )?;
        custody.adopt(observation.proof.capacity() as u64)?;
        drop(scope);
        custody.relinquish(held)?;
        custody.transfer();
        Ok(observation)
    }
}

/// Reducer-side verifier for host-issued scopes.
#[derive(Clone)]
pub struct AuthorityVerifier {
    pub(in crate::core) id: String,
    public: std::sync::Arc<runtime_signing::PublicAuthority>,
    admission: runtime_signing::AdmissionPort,
    pub(in crate::core) audience: Authority,
}

impl PartialEq for AuthorityVerifier {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.audience == other.audience
            && self.public.metadata_key == other.public.metadata_key
            && (std::sync::Arc::ptr_eq(&self.public, &other.public)
                || match (self.public.trust.read(), other.public.trust.read()) {
                    (Ok(left), Ok(right)) => *left == *right,
                    _ => false,
                })
    }
}
impl Eq for AuthorityVerifier {}

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
    /// Reject expired or revoked planning authority before any provider callback.
    pub(crate) fn verify_effect_plan(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<acyclic_auth::verify::VerifiedCapability> {
        let event = effect
            .planning
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("effect has no planning workload".into()))?;
        match &event.payload {
            EventPayload::EffectPlanned {
                effect_id: planned_id,
                provider,
                guarantee,
                effect_kind,
                request,
                request_digest,
                result_schema,
                result_schema_digest,
            } if *planned_id == effect_id
                && provider == &effect.provider
                && *guarantee == effect.guarantee
                && effect_kind == &effect.effect_kind
                && request == &effect.request
                && request_digest == &effect.request_digest
                && result_schema.as_ref() == &effect.result_schema
                && result_schema_digest == &effect.result_schema_digest => {}
            _ => {
                return Err(Error::Unauthorized(
                    "effect differs from its authenticated plan".into(),
                ));
            }
        }
        if !event.scope.capabilities.contains("effect:plan")
            || !event
                .scope
                .capabilities
                .contains(&capability::effect_provider(&effect.provider))
        {
            return Err(Error::Unauthorized(
                "planning workload does not grant this provider".into(),
            ));
        }
        workload::live_record(self, event, work)
    }

    /// Builds a verification-only owner from independently authenticated public configuration.
    /// This value cannot admit events, issue workloads or sign checkpoints.
    pub fn from_public(
        id: String,
        audience: Authority,
        trust: acyclic_auth::trust::Snapshot,
        checkpoint_key: [u8; 32],
    ) -> Result<Self> {
        audience.validate_with_work(&mut None)?;
        if id != trust.account {
            return Err(Error::Unauthorized(
                "public authority account mismatch".into(),
            ));
        }
        ed25519_dalek::VerifyingKey::from_bytes(&checkpoint_key)
            .map_err(|_| Error::Unauthorized("invalid checkpoint verification key".into()))?;
        Ok(Self {
            id,
            audience,
            public: std::sync::Arc::new(runtime_signing::PublicAuthority {
                trust: std::sync::RwLock::new(trust),
                metadata_key: checkpoint_key,
            }),
            admission: runtime_signing::AdmissionPort::closed(),
        })
    }

    /// Public checkpoint verification key, authenticated by the runtime configuration.
    #[must_use]
    pub fn public_checkpoint_key(&self) -> [u8; 32] {
        self.public.metadata_key
    }

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

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn admit_native_descendant(
        &self,
        parent: &Scope,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Scope> {
        let runtime = self.admission.runtime()?;
        AuthorityIssuer {
            id: self.id.clone(),
            audience: self.audience.clone(),
            runtime,
        }
        .issue_derived(parent, id, capabilities, agent, work)
    }

    /// Copies the verifier using the caller's ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        match work.as_deref_mut() {
            Some(work) => Ok(Self {
                id: work.clone_text(&self.id)?,
                public: self.public.clone(),
                admission: self.admission.clone(),
                audience: work.clone_authority(&self.audience)?,
            }),
            None => Ok(self.clone()),
        }
    }

    /// Verifies issuer identity and the complete immutable grant.
    pub fn verify(&self, scope: &Scope, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.verify_scope_for_with_work(&self.audience, scope, work)
    }

    /// Rebinds only the authenticated audience, retaining this original key.
    pub(crate) fn verify_scope_for_with_work(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        workload::verify(self, audience, scope, workload::now(), work).map(|_| ())
    }

    pub(in crate::core) fn canonical_digest<T: Serialize>(
        &self,
        domain: &[u8],
        value: &T,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut quota = CodecQuota::new(work, 0)?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            value,
            crate::store::default_projection_read_limits().maximum_bytes,
            quota.work(),
        )?;
        quota.adopt(bytes.capacity() as u64)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(domain.len() as u64 + 8 + bytes.len() as u64)?;
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
        let proof = *hasher.finalize().as_bytes();
        drop(bytes);
        quota.relinquish(quota.retained())?;
        Ok(proof)
    }

    pub(in crate::core) fn canonical_proof<T: Serialize>(
        &self,
        domain: &[u8],
        value: &T,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 64]> {
        let digest = self.canonical_digest(domain, value, work)?;
        self.admission.sign(&digest)
    }

    pub(in crate::core) fn verify_canonical_proof<T: Serialize>(
        &self,
        domain: &[u8],
        value: &T,
        proof: &[u8],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_metadata(&self.canonical_digest(domain, value, work)?, proof)
    }

    fn verify_metadata(&self, digest: &[u8; 32], proof: &[u8]) -> Result<()> {
        let key = ed25519_dalek::VerifyingKey::from_bytes(&self.public.metadata_key)
            .map_err(|_| Error::Unauthorized("invalid runtime verification key".into()))?;
        let signature = ed25519_dalek::Signature::from_slice(proof)
            .map_err(|_| Error::Unauthorized("invalid runtime signature".into()))?;
        key.verify_strict(digest, &signature).map_err(|_| {
            Error::Unauthorized("runtime signature does not authenticate this payload".into())
        })
    }

    pub(in crate::core) fn attest_event(
        &self,
        scope: &Scope,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Vec<u8>> {
        let digest = self.event_digest(event, work)?;
        self.admission
            .record(&self.id, &self.audience, scope, digest, work)
    }

    fn event_digest(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        crate::executor::json::canonical_json_digest(
            &(
                "harness/v2/committed-event",
                &self.audience,
                event.revision,
                event.operation_id,
                event.intent_digest,
                &event.scope,
                &event.causal_parent,
                &event.payload,
            ),
            crate::store::default_projection_read_limits().maximum_bytes,
            work,
        )
    }

    pub(crate) fn verify_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if event.scope.issuer != self.id {
            return Err(Error::Unauthorized(
                "event admission attestation is invalid".into(),
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let recheck = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(|work| crate::executor::PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let verified = match recheck.as_ref() {
            Some(io) => self.verify_event_record(event, &mut Some(&mut io.history_io())),
            None => self.verify_event_record(event, work),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let verified = self.verify_event_record(event, work);
        verified
    }

    fn verify_event_record(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let digest = self.event_digest(event, work)?;
        workload::verify_record(
            self,
            &event.attestation,
            digest,
            Some(&event.scope),
            &[],
            work,
        )
    }

    fn effect_history_root_digest_with_work(
        &self,
        revision: u64,
        canonical_digest: [u8; 32],
        root: Option<[u8; 32]>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.audience.validate_with_work(work)?;
        let domain = b"harness/v2/effect-history-root\0";
        let texts = [self.id.as_str(), self.audience.id.as_str()];
        let bytes = texts
            .iter()
            .try_fold(
                (domain.len() + 1 + 8 + 32 + 1 + 32) as u64,
                |total, text| total.checked_add(8)?.checked_add(text.len() as u64),
            )
            .ok_or_else(|| Error::Invalid("effect proof byte count overflows".into()))?;
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(bytes)?;
        }
        #[cfg(all(
            any(test, feature = "test-support"),
            feature = "filesystem",
            not(target_arch = "wasm32")
        ))]
        acyclic_differential::record(crate::test_support::EffectRootObservation::Hash(bytes));
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        hasher.update(&[match self.audience.kind {
            AggregateKind::Agent => 1,
            AggregateKind::Conversation => 2,
            AggregateKind::Session => 3,
            AggregateKind::Turn => 4,
            AggregateKind::Task => 5,
        }]);
        for text in texts {
            hasher.update(&(text.len() as u64).to_le_bytes());
            hasher.update(text.as_bytes());
        }
        hasher.update(&revision.to_le_bytes());
        hasher.update(&canonical_digest);
        hasher.update(&[u8::from(root.is_some())]);
        hasher.update(&root.unwrap_or([0; 32]));
        Ok(*hasher.finalize().as_bytes())
    }

    pub(crate) fn effect_history_root_proof_with_work(
        &self,
        revision: u64,
        canonical: [u8; 32],
        root: Option<[u8; 32]>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 64]> {
        self.admission
            .sign(&self.effect_history_root_digest_with_work(revision, canonical, root, work)?)
    }

    pub(crate) fn verify_effect_history_root_with_work(
        &self,
        revision: u64,
        canonical: [u8; 32],
        root: Option<[u8; 32]>,
        proof: &[u8],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_metadata(
            &self.effect_history_root_digest_with_work(revision, canonical, root, work)?,
            proof,
        )
    }

    fn fork_history_digest(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.verify_audience(&seed.parent, work)?;
        self.canonical_digest(
            b"harness/v2/fork-history-witness\0",
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
                    &seed.authorized_source,
                    &seed.child_result_volume,
                    &seed.child_result_generation,
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

    pub(crate) fn fork_history_proof(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 64]> {
        self.admission.sign(&self.fork_history_digest(seed, work)?)
    }

    pub(crate) fn verify_fork_history_proof(
        &self,
        seed: &ForkSeed,
        proof: &[u8],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_metadata(&self.fork_history_digest(seed, work)?, proof)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn fork_history_selection_proof_with_work(
        &self,
        revision: u64,
        through_sequence: u64,
        agent: Option<AgentId>,
        history_digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        self.canonical_digest(
            b"harness/v1/fork-history-selection\0",
            &(
                &self.id,
                self.public.metadata_key,
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
        work: &mut Option<&mut dyn HistoryIo>,
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
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let digest = effect_attestation_digest(&self.audience, observation, work)?;
        let provider = capability::effect_provider(&observation.provider);
        workload::verify_record(
            self,
            &observation.proof,
            digest,
            None,
            &["effect:run", &provider],
            work,
        )
    }
}

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "effect_root_tests.rs"]
pub(crate) mod effect_root_tests;

fn effect_attestation_digest(
    audience: &Authority,
    observation: &EffectAttestation,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 32]> {
    let mut custody = CodecQuota::new(work, 0)?;
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
    let proof = *blake3::hash(&bytes).as_bytes();
    drop(bytes);
    Ok(proof)
}
