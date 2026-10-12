use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Publishes this overlay with its permit, original context, and owner check.
    pub async fn commit<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<OperationReceipt<LivePublicationObservation>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(
            move |budget| async move {
                if self.mode.access != AccessMode::ReadWrite
                    || self.mode.mutations != MutationMode::PrivateOverlay
                {
                    return Err(OperationFailure::before_work(E::from(
                        FsError::MutationNotAllowed,
                    )));
                }
                self.publish_pending(
                    operation_id,
                    operation_context,
                    operation_evidence,
                    original_fingerprint_permit,
                    permit,
                    budget,
                    cancellation,
                    &mut before_publication,
                )
                .await
            },
            budget,
            cancellation,
            E::from(FsError::Cancelled(crate::CancellationError)),
            None,
            |error| E::from(FsError::from(error)),
        )
        .await
    }

    /// Seals the original companion operation even when its authored root is unchanged.
    pub(crate) async fn commit_even_if_clean<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> crate::MeasuredResult<OperationReceipt<LivePublicationObservation>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(
            move |budget| async move {
                if self.mode.access != AccessMode::ReadWrite
                    || self.mode.mutations != MutationMode::PrivateOverlay
                {
                    return Err(OperationFailure::before_work(E::from(
                        FsError::MutationNotAllowed,
                    )));
                }
                let expected = self.authority_head.ok_or_else(|| {
                    OperationFailure::before_work(E::from(FsError::WritableCheckoutRequiresHead))
                })?;
                self.publish_pending_against(
                    operation_id,
                    operation_context,
                    operation_evidence,
                    original_fingerprint_permit,
                    permit,
                    expected,
                    budget,
                    cancellation,
                    before_publication,
                    None,
                    producer_custody,
                )
                .await
            },
            budget,
            cancellation,
            E::from(FsError::Cancelled(crate::CancellationError)),
            None,
            |error| E::from(FsError::from(error)),
        )
        .await
    }

    pub(super) async fn publish_pending<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> crate::MeasuredResult<OperationReceipt<LivePublicationObservation>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + ProviderTask,
    {
        if !self.has_pending_mutations()
            && permit == PublicationPermit::Unrestricted
            && operation_context.is_none()
            && operation_evidence.is_none()
        {
            return self
                .resolve_clean_commit(operation_id, budget, cancellation)
                .await
                .map_err(|failure| failure.map_error(E::from));
        }
        let expected = self.authority_head.ok_or_else(|| {
            OperationFailure::before_work(E::from(FsError::WritableCheckoutRequiresHead))
        })?;
        self.publish_pending_against(
            operation_id,
            operation_context,
            operation_evidence,
            original_fingerprint_permit,
            permit,
            expected,
            budget,
            cancellation,
            before_publication,
            None,
            None,
        )
        .await
    }
}
