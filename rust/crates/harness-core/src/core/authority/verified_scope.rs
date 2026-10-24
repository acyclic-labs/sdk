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
                &*self
                    .verifier
                    .public
                    .trust
                    .read()
                    .map_err(workload::unauthorized)?,
                timestamp,
            )
            .map_err(workload::unauthorized)
    }
}
