//! One immutable scope paired with the evidence that authenticated it.
use super::*;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct VerifiedScope {
    pub(super) scope: Scope,
    pub(super) verifier: AuthorityVerifier,
    pub(super) evidence: acyclic_auth::verify::TrustEvidence,
}

impl AuthorityVerifier {
    /// Original authenticated trust publisher, without signing custody.
    /// Register before checking authority; producer loss ends active use.
    #[must_use]
    pub fn trust_changes(&self) -> std::sync::Arc<dyn acyclic_auth::trust::ChangeSource> {
        self.public.changes.clone()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn bind_checkpoint_stop(&mut self, stop: &acyclic_actors::guest::CancellationToken) {
        if let runtime_signing::AdmissionPort::Platform(_, _, checkpoint_stop) = &mut self.admission
        {
            *checkpoint_stop = Some(stop.clone());
        }
    }

    pub(crate) fn authenticate_scope(
        &self,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<VerifiedScope> {
        let capability = workload::verify(self, &self.audience, scope, self.public.now(), work)?;
        let mut quota = CodecQuota::new(work, 0)?;
        let evidence = compact_evidence(&capability, &mut quota)?;
        let scope = scope.copy(quota.work())?;
        quota.adopt(scope.logical_owned_bytes()?)?;
        let verifier = self.copy(quota.work())?;
        quota.adopt(verifier.logical_owned_bytes()?)?;
        let result = VerifiedScope {
            scope,
            verifier,
            evidence,
        };
        quota.transfer();
        Ok(result)
    }
}

// The caller's guard owns the evidence through all later construction failures.
fn compact_evidence(
    capability: &acyclic_auth::verify::VerifiedCapability,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<acyclic_auth::verify::TrustEvidence> {
    let storage = capability
        .trust_evidence_storage()
        .map_err(workload::unauthorized)?;
    if let Some(io) = quota.work().as_deref_mut() {
        for bytes in storage.allocation_bytes() {
            io.admit_allocation(bytes)?;
        }
        io.copy_bytes(storage.copied_bytes)?;
    }
    // Fund every copied buffer before evidence construction allocates.
    quota.replace(storage.owned_bytes)?;
    let evidence = capability
        .trust_evidence()
        .map_err(workload::unauthorized)?;
    quota.replace(evidence.owned_bytes().map_err(workload::unauthorized)?)?;
    Ok(evidence)
}
impl VerifiedScope {
    pub(crate) const fn scope(&self) -> &Scope {
        &self.scope
    }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn verifier(&self) -> &AuthorityVerifier {
        &self.verifier
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn copy_verifier(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        self.verifier.copy(work)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::ownership::owned_sum([
            self.scope.logical_owned_bytes()?,
            self.verifier.logical_owned_bytes()?,
            self.evidence
                .owned_bytes()
                .map_err(workload::unauthorized)?,
        ])
    }

    pub(crate) fn recheck(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.check_at(self.verifier.public.now(), work)
    }

    fn check_at(&self, timestamp: u64, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        self.verifier.admission.check(&self.scope)?;
        self.evidence
            .check(
                self.verifier
                    .public
                    .trust
                    .read()
                    .map_err(workload::unauthorized)?
                    .as_ref(),
                timestamp,
            )
            .map_err(workload::unauthorized)?;
        self.verifier.admission.check(&self.scope)
    }
}

impl AuthorityVerifier {
    /// Builds a verification-only owner from independently authenticated public configuration.
    /// This value cannot admit events, issue workloads or sign checkpoints.
    /// `changes` must follow the same authenticated owner's trust publications,
    /// never a new local channel. The snapshot remains public historical evidence;
    /// live admission also rechecks its original owner's exact validity guard.
    pub fn from_public(
        id: String,
        audience: Authority,
        trust: acyclic_auth::trust::Snapshot,
        checkpoint_key: [u8; 32],
        changes: std::sync::Arc<dyn acyclic_auth::trust::ChangeSource>,
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
                trust: std::sync::RwLock::new(std::sync::Arc::new(trust)),
                metadata_key: checkpoint_key,
                workload: Default::default(),
                changes,
                clock: None,
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
        AuthorityRoot {
            id: self.id.clone(),
            audience: self.audience.clone(),
            runtime,
        }
        .issue_derived(parent, id, capabilities, agent, work)
    }

    /// Copies the verifier using the caller's ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        CodecQuota::produce(work, |quota| self.copy_in_quota(quota)).map(|(verifier, _)| verifier)
    }

    pub(crate) fn copy_in_quota(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        if quota.work().is_none() {
            return Ok(self.clone());
        }
        Ok(Self {
            id: copying::copy_text(&self.id, quota)?,
            public: self.public.clone(),
            admission: self.admission.clone(),
            audience: copying::copy_authority(&self.audience, quota)?,
        })
    }

    /// Verifies issuer identity and the complete immutable grant.
    pub fn verify(&self, scope: &Scope, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.verify_scope_for_with_work(&self.audience, scope, work)
    }

    /// Verifies `scope` for this audience and returns its authenticated
    /// expiry in Unix seconds, the bound a command acting under it stops at.
    pub fn expiry(&self, scope: &Scope) -> Result<u64> {
        self.scope_expiry(&self.audience, scope, &mut None)
    }

    /// Rebinds only the authenticated audience, retaining this original key.
    pub(crate) fn verify_scope_for_with_work(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.scope_expiry(audience, scope, work).map(|_| ())
    }

    /// The authenticated half-open bound, including every ancestor and signer.
    /// Native ownership subscriptions use the same verifier and trusted clock.
    pub(crate) fn scope_expiry(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<u64> {
        workload::verify(self, audience, scope, self.public.now(), work)
            .map(|capability| capability.effective().expires_at_unix_seconds())
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

    pub(super) fn verify_metadata(&self, digest: &[u8; 32], proof: &[u8]) -> Result<()> {
        let key = ed25519_dalek::VerifyingKey::from_bytes(&self.public.metadata_key)
            .map_err(|_| Error::Unauthorized("invalid runtime verification key".into()))?;
        acyclic_auth::crypto::verify_signature(&key, digest, proof).map_err(|_| {
            Error::Unauthorized("runtime signature does not authenticate this payload".into())
        })
    }

    fn checkpoint_proof(
        &self,
        kind: CheckpointKind,
        digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> impl std::future::Future<Output = Result<[u8; 64]>> + use<> {
        let prepared = match &self.admission {
            runtime_signing::AdmissionPort::Native(_) => futures::future::Either::Left(
                self.canonical_proof(kind.domain(), &(&self.id, &self.audience, digest), work),
            ),
            runtime_signing::AdmissionPort::Platform(owner, operation, stop) => {
                let owner: Result<_> = (|| {
                    if let Some(work) = work.as_deref() {
                        work.check_cancellation()?;
                    }
                    let owner = owner.upgrade().ok_or_else(|| {
                        Error::Unauthorized("platform authority owner is closed".into())
                    })?;
                    owner.check()?;
                    Ok((owner, *operation, stop.clone()))
                })();
                futures::future::Either::Right(owner)
            }
        };
        async move {
            match prepared {
                futures::future::Either::Left(result) => result,
                futures::future::Either::Right(owner) => {
                    let (owner, operation, stop) = owner?;
                    let proof = owner.checkpoint(&owner.context, operation, kind, digest);
                    until_turn_stopped(stop, proof).await
                }
            }
        }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn execution_checkpoint_proof(
        &self,
        digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> impl std::future::Future<Output = Result<[u8; 64]>> + use<> {
        self.checkpoint_proof(CheckpointKind::Execution, digest, work)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_execution_checkpoint(
        &self,
        digest: [u8; 32],
        proof: &[u8; 64],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_canonical_proof(
            b"harness/v2/execution-index-checkpoint\0",
            &(&self.id, &self.audience, digest),
            proof,
            work,
        )
    }

    pub(crate) fn working_checkpoint_proof(
        &self,
        digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> impl std::future::Future<Output = Result<[u8; 64]>> + use<> {
        self.checkpoint_proof(CheckpointKind::Working, digest, work)
    }

    pub(crate) fn verify_working_checkpoint(
        &self,
        digest: [u8; 32],
        proof: &[u8; 64],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_canonical_proof(
            b"harness/v2/working-context-checkpoint\0",
            &(&self.id, &self.audience, digest),
            proof,
            work,
        )
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn model_request_proof(
        &self,
        digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> impl std::future::Future<Output = Result<[u8; 64]>> + use<> {
        self.checkpoint_proof(CheckpointKind::ModelRequest, digest, work)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn authenticates_model_request(
        &self,
        digest: [u8; 32],
        proof: &[u8; 64],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let digest = self.canonical_digest(
            b"harness/v2/model-request-checkpoint\0",
            &(&self.id, &self.audience, digest),
            work,
        )?;
        // Invalid signatures fall back to reconstructing the request. Admission
        // and cancellation failures above must still propagate to the caller.
        Ok(self.verify_metadata(&digest, proof).is_ok())
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
        Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Unauthorized,
            "authority verifier belongs to another aggregate",
        ))
    }
}
