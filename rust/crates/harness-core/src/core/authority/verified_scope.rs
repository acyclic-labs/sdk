//! One immutable scope paired with the evidence that authenticated it.
use super::*;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct VerifiedScope {
    scope: Scope,
    verifier: AuthorityVerifier,
    evidence: acyclic_auth::verify::TrustEvidence,
}

impl AuthorityVerifier {
    pub(crate) fn authenticate_scope(
        &self,
        scope: &Scope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<VerifiedScope> {
        let capability = workload::verify(self, &self.audience, scope, workload::now(), work)?;
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
        self.check_at(workload::now(), work)
    }

    fn check_at(&self, timestamp: u64, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
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
            .map_err(workload::unauthorized)
    }
}

impl AuthorityVerifier {
    /// Reject expired or revoked planning authority before any provider callback.
    #[cfg(test)]
    pub(crate) fn verify_effect_plan(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<std::sync::Arc<workload::AuthenticatedRecord>> {
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
                trust: std::sync::RwLock::new(std::sync::Arc::new(trust)),
                metadata_key: checkpoint_key,
                workload: Default::default(),
                historical: Default::default(),
                revision: tokio::sync::watch::channel(0).0,
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
        workload::verify(self, audience, scope, workload::now(), work)
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

    pub(super) fn event_digest(
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

    #[cfg(feature = "filesystem")]
    pub(crate) fn execution_checkpoint_proof(
        &self,
        digest: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 64]> {
        self.canonical_proof(
            b"harness/v2/execution-index-checkpoint\0",
            &(&self.id, &self.audience, digest),
            work,
        )
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
    ) -> Result<[u8; 64]> {
        self.canonical_proof(
            b"harness/v2/working-context-checkpoint\0",
            &(&self.id, &self.audience, digest),
            work,
        )
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
    ) -> Result<[u8; 64]> {
        self.canonical_proof(
            b"harness/v2/model-request-checkpoint\0",
            &(&self.id, &self.audience, digest),
            work,
        )
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

    pub(crate) fn verify_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.authenticated_event(event, work).map(|_| ())
    }

    pub(super) fn authenticated_event(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<std::sync::Arc<workload::AuthenticatedRecord>> {
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

    pub(super) fn verify_event_record(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<std::sync::Arc<workload::AuthenticatedRecord>> {
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

    pub(super) fn effect_history_root_digest_with_work(
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

    pub(super) fn fork_history_digest(
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

    pub(crate) fn verify_fork_history_proof(
        &self,
        seed: &ForkSeed,
        proof: &[u8],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.verify_metadata(&self.fork_history_digest(seed, work)?, proof)
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
        .map(|_| ())
    }
}
