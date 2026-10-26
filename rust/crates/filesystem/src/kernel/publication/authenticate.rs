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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let (proof, guard, pending) = ProofContext::new(
            objects,
            limits,
            ProofMeter::Native {
                original,
                cancellation,
            },
        )
        .prove_cached_from(root, cache, authority_root)
        .await?;
        return Ok(Authenticated {
            proof,
            guard,
            pending,
        });
    }
    let closure_failure =
        |error| crate::FsError::Closure(crate::kernel::closure::closure_error(error));
    let meter =
        ProofMeter::standalone(accounting.remaining()?, cancellation).map_err(closure_failure)?;
    let result = ProofContext::new(objects, limits, meter.clone())
        .prove_cached_from(root, cache, authority_root)
        .await;
    accept_backend(accounting, meter.work())?;
    let (proof, guard, pending) = result.map_err(closure_failure)?;
    retain(accounting, guard.retained_bytes())?;
    Ok(Authenticated {
        proof,
        guard,
        pending,
    })
}

pub(super) fn retire(
    output: (GenerationProof, ProofGuard),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let bytes = output.1.retained_bytes();
    drop(output.0);
    drop(output.1);
    if let Some((allocations, _, _, _)) = accounting.standalone_parts() {
        allocations
            .release(bytes)
            .map_err(|_| WorkError::Overflow)?;
    }
    Ok(())
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
fn retain_authentication(
    authenticated: Authenticated,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::allocation::retained::Retained<Authenticated>, crate::FsError> {
    if accounting.standalone_parts().is_some() {
        let bytes = authenticated.guard.retained_bytes();
        crate::kernel::allocation::retained::Retained::from_owned(
            Owned::new(authenticated, KernelOwnedAllocation::Standalone, bytes),
            accounting,
        )
    } else {
        // Native proof guards already own their original producer's allocation.
        Ok(crate::kernel::allocation::retained::Retained {
            value: authenticated,
            allocation: crate::kernel::allocation::retained::RetainedAllocation::claim(
                0, 0, accounting,
            )?,
        })
    }
}
