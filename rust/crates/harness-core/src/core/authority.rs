//! Aggregate identity and host-issued authorization.
use super::*;
mod admitted;
mod runtime;
mod runtime_signing;
pub use admitted::{AdmittedAuthority, AuthorityOperations, CheckpointKind, ScopeDerivation};
pub use runtime::RuntimeAuthority;
pub(crate) use runtime::until_turn_stopped;
mod verified_scope;
mod workload;
pub(crate) use verified_scope::VerifiedScope;
/// Kind of independently ordered durable aggregate.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, crate::HeapLayout,
)]
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
#[derive(
    Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, crate::HeapLayout,
)]
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
            return Err(crate::store::guarded_boundary(
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
#[derive(Clone, Eq, Serialize, Deserialize, crate::HeapLayout)]
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
    /// Ed25519 workload chain with a restricted append tip or a public seal.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    #[serde(with = "super::capability_bytes")]
    #[heap_layout(as = crate::heap_layout::markers::Base64Bytes)]
    pub(in crate::core) workload: Vec<u8>,
}

impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        // Workload bytes include signatures and the restricted append seed.
        // Keep credential comparisons on the shared constant-time path too.
        acyclic_auth::crypto::authentication_eq(&self.workload, &other.workload)
            && acyclic_auth::crypto::authentication_eq(self.proof.as_ref(), other.proof.as_ref())
            && self.id == other.id
            && self.capabilities == other.capabilities
            && self.agent == other.agent
            && self.issuer == other.issuer
            && self.parent_proof == other.parent_proof
    }
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

    /// Replace the restricted append tip with public verification evidence.
    /// Hosts do this before exporting a scope to a guest. The scope's claims,
    /// proof identity and revocation dependencies remain unchanged; subsequent
    /// delegation requires its custodian's admitted operation port.
    pub fn seal(mut self, verifier: &AuthorityVerifier) -> Result<Self> {
        let capability = workload::verify(
            verifier,
            &verifier.audience,
            &self,
            verifier.public.now(),
            &mut None,
        )?;
        if !capability.is_sealed() {
            let mut encoded = capability.encode_sealed().map_err(workload::unauthorized)?;
            zeroize::Zeroize::zeroize(&mut self.workload);
            self.workload = std::mem::take(&mut *encoded);
        }
        Ok(self)
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
        CodecQuota::produce(work, |quota| self.copy_in_quota(quota)).map(|(scope, _)| scope)
    }

    pub(crate) fn copy_in_quota(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        copy(self, quota, |scope, _| scope.logical_owned_bytes())
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

/// Bootstrap custody owned by the runtime composition, never by a workload.
/// Workloads receive an `AuthorityIssuer` and an authenticated parent scope.
#[derive(Clone)]
pub struct AuthorityRoot {
    pub(in crate::core) id: String,
    runtime: std::sync::Arc<runtime_signing::RuntimeSigner>,
    pub(in crate::core) audience: Authority,
}

/// Delegation authority: public trust and a required parent append capability.
/// This handle never retains a runtime root signing key.
#[derive(Clone)]
pub struct AuthorityIssuer {
    verifier: AuthorityVerifier,
}

impl AuthorityIssuer {
    /// Issue an authenticated descendant that can only narrow its parent.
    /// The agent, issuer, ancestry, expiry and revocation chain are preserved.
    pub fn issue(
        &self,
        parent: &Scope,
        id: impl Into<String>,
        capabilities: &Capabilities,
    ) -> Result<Scope> {
        let trust = self
            .verifier
            .public
            .trust
            .read()
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))?
            .clone();
        parent.attenuate_workload(
            id,
            capabilities,
            trust.as_ref(),
            &self.verifier.audience,
            self.verifier.public.now(),
        )
    }
}

impl AuthorityRoot {
    /// Bootstrap a native authority from OS-owned signing custody. Host records
    /// retain only a reference; delegation receives the parent-bound issuer below.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bootstrap(
        id: impl Into<String>,
        reference: &acyclic_native_runtime::signing_key::SigningKeyReference,
        custody: acyclic_native_runtime::signing_key::SigningCustody<'_>,
        audience: Authority,
    ) -> Result<Self> {
        reference
            .with_seed(custody, |seed| Self::new(id, *seed, audience))
            .map_err(|error| Error::Unauthorized(error.to_string()))
    }

    /// Creates an issuer from host-managed key material.
    #[must_use]
    pub fn new(id: impl Into<String>, key: [u8; 32], audience: Authority) -> Self {
        Self::create(id.into(), key, audience, None)
    }

    /// Creates host custody with its original trusted wall clock. Issuance,
    /// delegation, verifier clones and retained authority checks share this
    /// owner. The clock cannot be replaced after publication or supplied by a
    /// guest request. Milliseconds are evaluated as Unix seconds by Auth.
    #[must_use]
    pub fn with_clock(
        id: impl Into<String>,
        key: [u8; 32],
        audience: Authority,
        clock: std::sync::Arc<dyn acyclic_stream::UnixMillisClock>,
    ) -> Self {
        Self::create(id.into(), key, audience, Some(clock))
    }

    fn create(
        id: String,
        key: [u8; 32],
        audience: Authority,
        clock: Option<std::sync::Arc<dyn acyclic_stream::UnixMillisClock>>,
    ) -> Self {
        Self {
            runtime: std::sync::Arc::new(runtime_signing::RuntimeSigner::new(&id, key, clock)),
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

    #[cfg(test)]
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
            .map(|trust| (**trust).clone())
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))
    }

    /// The public trust with its revision, read together: a revocation
    /// publishes its revision while it holds the trust exclusively.
    pub fn public_trust_revision(
        &self,
    ) -> Result<(std::sync::Arc<acyclic_auth::trust::Snapshot>, u64)> {
        let trust = self
            .runtime
            .public
            .trust
            .read()
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))?;
        Ok((
            std::sync::Arc::clone(&trust),
            *self.runtime.revision.borrow(),
        ))
    }

    /// Announces each change of the public trust, such as a revocation.
    #[must_use]
    pub fn trust_revisions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.runtime.revision.subscribe()
    }

    /// Original authenticated trust event owner, without signing custody.
    pub fn trust_changes(&self) -> std::sync::Arc<dyn acyclic_auth::trust::ChangeSource> {
        self.runtime.public.changes.clone()
    }

    /// Revokes this workload and every descendant at this runtime's public trust boundary.
    pub fn revoke_workload(&self, scope: &Scope) -> Result<()> {
        self.verifier(&mut None)?.verify(scope, &mut None)?;
        let mut trust = self
            .runtime
            .public
            .trust
            .write()
            .map_err(|_| Error::Storage("runtime trust poisoned".into()))?;
        let revision = self
            .runtime
            .revision
            .borrow()
            .checked_add(1)
            .ok_or_else(|| Error::Storage("runtime trust revision overflow".into()))?;
        std::sync::Arc::make_mut(&mut trust)
            .revoked_block_ids
            .insert(*scope.proof);
        self.runtime.revision.send_replace(revision);
        Ok(())
    }

    /// Establishes a root scope at the trusted runtime bootstrap boundary.
    /// Delegated callers use `AuthorityIssuer::issue`, which requires a parent.
    pub fn bootstrap_scope(
        &self,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Scope> {
        self.issue_bound(id, capabilities, agent, None, None, work)
    }

    /// The same signing custody for another audience, such as one agent's
    /// conversation. It shares this root's signer and trust.
    #[must_use]
    pub fn with_audience(&self, audience: Authority) -> Self {
        Self {
            id: self.id.clone(),
            runtime: std::sync::Arc::clone(&self.runtime),
            audience,
        }
    }

    /// Gives a workload a delegation handle containing no runtime signing key.
    pub fn issuer(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<AuthorityIssuer> {
        Ok(AuthorityIssuer {
            verifier: self.verifier(work)?,
        })
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
            self.runtime.public.now(),
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

    /// The `harness:/` resource of this root's audience, which every
    /// credential it issues is bound to.
    pub fn audience_resource(&self) -> Result<String> {
        workload::resource(&self.audience)
    }

    /// A credential for exactly `grants`, each `(resource, prefix, actions)`,
    /// under this root's audience, derived from `parent` at `now`: it lives
    /// no longer than `parent` and is revoked with it. Callers first check
    /// that `parent` may reach everything `grants` names. Returns it with
    /// the principal it authenticates.
    pub(crate) fn resource_credential(
        &self,
        parent: &Scope,
        grants: &[(&str, bool, &[&str])],
        now: u64,
    ) -> Result<(Vec<u8>, String)> {
        let capability = workload::verify(
            &self.verifier(&mut None)?,
            &self.audience,
            parent,
            now,
            &mut None,
        )?;
        workload::resource_credential(self, &capability, grants, now)
    }

    pub(in crate::core) fn issue_bound(
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
        self.issuer(&mut None)?.issue(parent, id, capabilities)
    }

    /// Delegates one pinned private file to another agent without a fork or
    pub fn delegate_private_file_read(
        &self,
        owner_scope: &Scope,
        reader: AgentId,
        id: impl Into<String>,
        file: &FileRef,
    ) -> Result<Scope> {
        self.delegate_read(
            owner_scope,
            &id.into(),
            ScopeDerivation::PrivateFileRead { file, reader },
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
        self.delegate_read(
            owner_scope,
            &id.into(),
            ScopeDerivation::PrivateDirectoryRead {
                volume,
                prefix,
                reader,
            },
        )
    }

    fn delegate_read(
        &self,
        parent: &Scope,
        id: &str,
        request: ScopeDerivation<'_>,
    ) -> Result<Scope> {
        self.verifier(&mut None)?.verify(parent, &mut None)?;
        let (capabilities, agent) = request.resolve(parent)?;
        self.issue_derived(parent, id, capabilities.as_ref(), agent, &mut None)
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

crate::core::ownership::owned_enum!(AggregateKind {
    Agent,
    Conversation,
    Session,
    Turn,
    Task,
});
crate::core::ownership::owned_struct!(Authority { kind, id });
crate::core::ownership::owned_struct!(Scope {
    id,
    capabilities,
    agent,
    issuer,
    parent_proof,
    proof,
    workload
});
