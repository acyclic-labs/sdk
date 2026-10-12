use super::*;

pub(super) enum WorkspaceJoinPublished {
    Committed(GenerationId), AlreadyCommitted(GenerationId), Conflict, Fenced, IdempotencyConflict,
}

pub(super) fn workspace_join_published(receipt: &crate::kernel::PublicationReceipt) -> WorkspaceJoinPublished {
    match &receipt.outcome {
        AppendOutcome::Committed(_) => WorkspaceJoinPublished::Committed(receipt.proof.generation_id),
        AppendOutcome::AlreadyCommitted(_) => WorkspaceJoinPublished::AlreadyCommitted(receipt.proof.generation_id),
        AppendOutcome::Conflict { .. } => WorkspaceJoinPublished::Conflict, AppendOutcome::Fenced { .. } => WorkspaceJoinPublished::Fenced,
        AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinPublished::IdempotencyConflict,
    }
}

pub(super) trait WorkspaceJoinPublication<A: AsyncAuthorityStore, O: AsyncObjectStore>: Send {
    type Error: From<FsError> + From<WorkspaceError> + Send;
    fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<WorkspaceJoinPublished, Self::Error>> + acyclic_stream::ProviderTask;
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> WorkspaceJoinPublication<A, O> for () {
    type Error = WorkspaceError;
    async fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPublished, Self::Error> {
        let published = publish_generation_async_with_permit(
            objects, authority, request, intent, limits, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &published { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        published.map(|receipt| workspace_join_published(&receipt)).map_err(|failure| WorkspaceError::from(FsError::from(failure.error)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'original, A, O, F, Fut, E> WorkspaceJoinPublication<A, O> for (&'original NativeMaterializationWork, &mut F)
where
    A: AsyncAuthorityStore, O: AsyncObjectStore,
    F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static, E: From<FsError> + From<WorkspaceError> + Send + 'static,
{
    type Error = E;
    async fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPublished, E> {
        let original = accounting.native_work().ok_or(FsError::MutationNotAllowed)?;
        if !original.shares_ledger(self.0) {
            return Err(E::from(FsError::from(NativeTreeMaterializationError::InvalidLayout)));
        }
        let (receipt, allocation) = publish_generation_async_guarded_with_native_work(
            objects, authority, request, intent, limits, self.0, cancellation, self.1, None, None,
        ).await?;
        let outcome = workspace_join_published(&receipt);
        // The kernel owns the sole preparation hook, publication hold and CAS.
        drop(receipt);
        drop(allocation);
        Ok(outcome)
    }
}

