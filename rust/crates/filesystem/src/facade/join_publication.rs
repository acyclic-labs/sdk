use super::*;

pub(super) fn checkout_commit_outcome(
    receipt: &crate::kernel::PublicationReceipt,
) -> LivePublicationObservation {
    let generation_id = receipt.proof.generation_id;
    match &receipt.outcome {
        AppendOutcome::Committed(commit) => LivePublicationObservation::Committed {
            generation_id,
            head: durable_head(commit),
        },
        AppendOutcome::AlreadyCommitted(commit) => LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head: durable_head(commit),
        },
        AppendOutcome::Conflict { actual } => {
            LivePublicationObservation::Conflict { actual: *actual }
        }
        AppendOutcome::Fenced { actual_epoch } => LivePublicationObservation::Fenced {
            actual_epoch: *actual_epoch,
        },
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint: *committed_fingerprint,
        },
    }
}

pub(super) trait WorkspaceJoinPublication<A: AsyncAuthorityStore, O: AsyncObjectStore>:
    Send
{
    type Error: From<FsError> + From<WorkspaceError> + Send;
    fn publish(
        &mut self,
        objects: &O,
        authority: &A,
        request: PublishGenerationRequest,
        intent: PublicationIntent,
        limits: crate::kernel::ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<LivePublicationObservation, Self::Error>>
    + acyclic_stream::ProviderTask;
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> WorkspaceJoinPublication<A, O> for () {
    type Error = WorkspaceError;
    async fn publish(
        &mut self,
        objects: &O,
        authority: &A,
        request: PublishGenerationRequest,
        intent: PublicationIntent,
        limits: crate::kernel::ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<LivePublicationObservation, Self::Error> {
        let published = publish_generation_async_with_permit(
            objects,
            authority,
            request,
            intent,
            limits,
            accounting.remaining()?,
            cancellation,
        )
        .await;
        let spent = match &published {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        merge_creation_backend_work(accounting, spent)?;
        published
            .map(|receipt| checkout_commit_outcome(&receipt))
            .map_err(|failure| WorkspaceError::from(FsError::from(failure.error)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'original, A, O, F, Fut, E> WorkspaceJoinPublication<A, O>
    for (&'original NativeMaterializationWork, &mut F)
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut
        + Clone
        + Send
        + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: From<FsError> + From<WorkspaceError> + Send + 'static,
{
    type Error = E;
    async fn publish(
        &mut self,
        objects: &O,
        authority: &A,
        request: PublishGenerationRequest,
        intent: PublicationIntent,
        limits: crate::kernel::ClosureLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<LivePublicationObservation, E> {
        let original = accounting
            .native_work()
            .ok_or(FsError::MutationNotAllowed)?;
        if !original.shares_ledger(self.0) {
            return Err(E::from(FsError::from(
                NativeTreeMaterializationError::InvalidLayout,
            )));
        }
        let (receipt, allocation) = publish_generation_async_guarded_with_native_work(
            objects,
            authority,
            request,
            intent,
            limits,
            self.0,
            cancellation,
            self.1,
            None,
            None,
        )
        .await?;
        let outcome = checkout_commit_outcome(&receipt);
        // The kernel owns the sole preparation hook, publication hold and CAS.
        drop(receipt);
        drop(allocation);
        Ok(outcome)
    }
}
