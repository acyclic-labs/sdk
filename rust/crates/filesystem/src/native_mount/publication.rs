//! One fenced publication path shared by native mounts and host watchers.

use crate::MountSourceError;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, CheckoutCommitOutcome,
    LiveMutationOutcome, OperationId, WorkBudget,
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
    if !checkout.has_pending_mutations() {
        return Ok(());
    }
    match checkout.mode().mutations {
        crate::model::MutationMode::PrivateOverlay => match checkout
            .commit(operation_id, budget, cancellation)
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
        crate::model::MutationMode::DirectLive => match checkout
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
        },
        crate::model::MutationMode::None => Err(MountSourceError::Unsupported(
            "checkout does not admit native writes".to_owned(),
        )),
    }
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string())
}
