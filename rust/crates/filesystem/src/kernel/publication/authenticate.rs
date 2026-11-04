use super::*;

pub(super) struct Authenticated {
    pub(super) proof: GenerationProof,
    pub(super) guard: ProofGuard,
    pub(super) pending: Option<crate::kernel::closure::PendingProof>,
}

pub(super) async fn prove<O: crate::AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    authority_root: Option<ObjectId>,
    limits: ClosureLimits,
    cache: Option<&Mutex<CachedProofs>>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Authenticated, crate::FsError> {
    use crate::kernel::closure::{ProofContext, ProofMeter};
    cancellation.check()?;
    let (proof, guard, pending) = {
        let mut frame = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let meter = ProofMeter::from_accounting(frame.accounting())?;
        ProofContext::new(objects, limits, meter)
            .prove_cached_from(root, cache, authority_root)
            .await?
    };
    Ok(Authenticated {
        proof,
        guard,
        pending,
    })
}

pub(super) struct PinnedAuthentication {
    pub(super) authentication: crate::kernel::allocation::retained::Retained<Authenticated>,
    pub(super) pin: Option<crate::collection::Hold>,
}

/// One admission-to-root-registry handoff for provisional and selected roots.
pub(super) async fn retain_and_pin<O: crate::AsyncObjectStore>(
    objects: &O,
    authenticated: Authenticated,
    config: crate::VolumeConfig,
    proven_at: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<PinnedAuthentication, crate::FsError> {
    let authentication = retain_authentication(authenticated, accounting)?;
    let pin = if let Some(collection) = objects.collection() {
        Some(
            collection
                .hold_generation_with_accounting(
                    &authentication.value.proof,
                    config,
                    proven_at,
                    accounting,
                )
                .await?,
        )
    } else {
        None
    };
    Ok(PinnedAuthentication {
        authentication,
        pin,
    })
}
pub(super) fn retain_authentication(
    authenticated: Authenticated,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::allocation::retained::Retained<Authenticated>, crate::FsError> {
    // The proof guard already owns its original producer's allocation.
    Ok(crate::kernel::allocation::retained::Retained {
        value: authenticated,
        allocation: crate::kernel::allocation::retained::RetainedAllocation::claim(
            0, 0, accounting,
        )?,
    })
}
