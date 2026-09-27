//! One fenced publication path shared by native mounts and host watchers.

use crate::MountSourceError;
use crate::kernel::RebaseDecision;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, CheckoutCommitOutcome,
    LiveMutationOutcome, OperationId, PublicationPermit, WorkBudget, WorkCounters,
};

/// Seals the exact current checkout candidate under one stable operation ID.
///
/// Callers must retain `operation_id` across every ambiguous retry and must not
/// admit another mutation until this function acknowledges success.
///
/// # Errors
///
/// Returns stale for deterministic conflicts/fences and an engine failure for
/// storage, authentication, cancellation, or indeterminate authority results.
pub async fn seal_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    seal_checkout_with_permit(
        checkout,
        operation_id,
        PublicationPermit::Unrestricted,
        budget,
        cancellation,
    )
    .await
}

/// Seals a checkout under one authority-evaluated operation permit.
pub async fn seal_checkout_with_permit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    if !checkout.has_pending_mutations() {
        return Ok(());
    }
    match checkout.mode().mutations {
        crate::model::MutationMode::PrivateOverlay => match checkout
            .commit_with_permit(operation_id, permit, budget, cancellation)
            .await
            .map_err(engine_error)?
            .value
        {
            CheckoutCommitOutcome::Committed { .. }
            | CheckoutCommitOutcome::AlreadyCommitted { .. } => Ok(()),
            CheckoutCommitOutcome::Conflict { .. }
            | CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => Err(MountSourceError::Stale),
        },
        crate::model::MutationMode::DirectLive if permit == PublicationPermit::Unrestricted => {
            match checkout
                .resume_live(operation_id, 8, 256, budget, cancellation)
                .await
                .map_err(engine_error)?
                .value
            {
                LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. } => Ok(()),
                LiveMutationOutcome::Conflicted { .. }
                | LiveMutationOutcome::RetryLimit { .. }
                | LiveMutationOutcome::Fenced { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. } => Err(MountSourceError::Stale),
            }
        }
        crate::model::MutationMode::DirectLive => Err(MountSourceError::Unsupported(
            "direct-live mounts do not support operation lease permits".to_owned(),
        )),
        crate::model::MutationMode::None => Err(MountSourceError::Unsupported(
            "checkout does not admit native writes".to_owned(),
        )),
    }
}

/// Publishes a checkout operation even when its only effect is the separately
/// journaled lazy source overlay.
pub async fn seal_checkout_with_permit_force<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    operation_id: OperationId,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), MountSourceError> {
    if checkout.mode().mutations != crate::model::MutationMode::PrivateOverlay {
        return Err(MountSourceError::Unsupported(
            "forced publication requires a private-overlay checkout".to_owned(),
        ));
    }
    const MAXIMUM_SAFE_REBASE_ATTEMPTS: usize = 8;
    let mut work = WorkCounters::default();
    for _ in 0..MAXIMUM_SAFE_REBASE_ATTEMPTS {
        let remaining = work.remaining(budget).map_err(engine_error)?;
        let publication = if checkout.has_pending_mutations() {
            checkout
                .commit_with_permit(operation_id, permit, remaining, cancellation)
                .await
        } else {
            checkout
                .commit_with_permit_even_if_clean(operation_id, permit, remaining, cancellation)
                .await
        }
        .map_err(engine_error)?;
        work = work.checked_add(publication.work).map_err(engine_error)?;
        work.verify(budget).map_err(engine_error)?;
        match publication.value {
            CheckoutCommitOutcome::Committed { .. }
            | CheckoutCommitOutcome::AlreadyCommitted { .. } => return Ok(()),
            CheckoutCommitOutcome::Conflict { .. } => {
                let maximum_conflicts = checkout
                    .volume_config()
                    .limits
                    .maximum_checkout_dependencies;
                let decision = checkout
                    .rebase_head(
                        maximum_conflicts,
                        work.remaining(budget).map_err(engine_error)?,
                        cancellation,
                    )
                    .await
                    .map_err(engine_error)?;
                work = work.checked_add(decision.work).map_err(engine_error)?;
                work.verify(budget).map_err(engine_error)?;
                if matches!(decision.value, RebaseDecision::Conflicted { .. }) {
                    return Err(MountSourceError::Stale);
                }
            }
            CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                return Err(MountSourceError::Stale);
            }
        }
    }
    Err(MountSourceError::Engine(
        "publication raced with repeated head advances; retry retained operation".to_owned(),
    ))
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
        VolumeConfig,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn conflicting_publication_retry_has_one_cumulative_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Manual,
            mutations: MutationMode::PrivateOverlay,
        };
        let mut first = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut stale = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut budgeted = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        for (checkout, name) in [
            (&mut first, b"first".as_slice()),
            (&mut stale, b"stale"),
            (&mut budgeted, b"stale"),
        ] {
            let path = NamespacePath::new(
                vec![LogicalName::new(
                    NameEncoding::Utf8,
                    name.to_vec(),
                    config.limits.maximum_component_bytes,
                )?],
                config.limits,
            )?;
            checkout
                .create_file(
                    path,
                    Bytes::from_static(b"data"),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?;
        }
        assert!(matches!(
            first
                .commit(
                    OperationId::from_bytes([1; 16]),
                    WorkBudget::UNBOUNDED,
                    &cancellation
                )
                .await?
                .value,
            CheckoutCommitOutcome::Committed { .. }
        ));
        let conflict = stale
            .commit(
                OperationId::from_bytes([2; 16]),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert!(matches!(
            conflict.value,
            CheckoutCommitOutcome::Conflict { .. }
        ));
        assert!(conflict.work.authority_records_read > 0);
        let rebase = stale
            .rebase_head(
                config.limits.maximum_checkout_dependencies,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert!(matches!(rebase.value, RebaseDecision::Safe { .. }));
        assert!(rebase.work.authority_records_read > 0);

        // Each step individually fits; their sum does not. A per-attempt
        // reset would admit work beyond the caller's budget.
        let budget = WorkBudget {
            authority_records_read: conflict
                .work
                .authority_records_read
                .max(rebase.work.authority_records_read),
            ..WorkBudget::UNBOUNDED
        };
        let result = seal_checkout_with_permit_force(
            &mut budgeted,
            OperationId::from_bytes([3; 16]),
            PublicationPermit::Unrestricted,
            budget,
            &cancellation,
        )
        .await;
        assert!(
            matches!(
                result,
                Err(MountSourceError::Engine(ref error)) if error.contains("authority_records_read")
            ),
            "{result:?}"
        );
        assert!(budgeted.has_pending_mutations());
        Ok(())
    }
}
