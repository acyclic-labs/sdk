use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn recover_publication<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        before_publication: &mut F,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        selection: (SelectedPublication<'_>, Option<SelectedImportProof>),
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<Option<crate::NativePublicationSnapshot>, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (selected, selected_proof) = selection;
        let remembered = self
            .removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .publication;
        if let (Some((operation, proof)), Some((_, _, requested))) = (remembered, selected)
            && (operation != requested || proof != selected_proof)
        {
            return Err(MountSourceError::Stale.into());
        }
        let retained = {
            let mut checkout = adapter::await_lock(
                self.authored.shared_checkout().lock(),
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await?;
            checkout
                .has_retained_operation()
                .then(|| checkout.retained_operation_id())
        };
        let operation = remembered.map(|(operation, _)| operation);
        let committed = match operation.or(selected.map(|(_, _, operation)| operation)) {
            Some(operation) => self
                .lazy
                .operation_generation(
                    IdempotencyKey::from_bytes(operation.into_bytes()),
                    accounting,
                )
                .await
                .map_err(lazy_error)?
                .map(|generation| (operation, generation)),
            None => None,
        };
        if let Some((operation, generation)) = committed {
            let proof = remembered.and_then(|(_, proof)| proof).or(selected
                .filter(|(_, _, requested)| *requested == operation)
                .and(selected_proof));
            return self
                .recover_committed_publication(
                    permit,
                    accounting,
                    before_publication,
                    acknowledged,
                    (
                        operation,
                        generation,
                        proof,
                        retained,
                        remembered.map(|(id, _)| id),
                    ),
                    accounting_failure,
                )
                .await
                .map(Some);
        }
        if let Some(operation) = operation {
            if retained == Some(operation) {
                return self
                    .recover_retained_publication(
                        permit,
                        accounting,
                        before_publication,
                        acknowledged,
                        (operation, remembered.and_then(|(_, proof)| proof)),
                        accounting_failure,
                    )
                    .await
                    .map(Some);
            }
            self.lazy
                .finish_mount_removals(accounting)
                .await
                .map_err(lazy_error)?;
            self.abort_detached_publication()?;
            self.removals
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .publication = None;
        }
        Ok(None)
    }
    async fn recover_committed_publication<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        before_publication: &mut F,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        committed: CommittedPublication,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);
        let (operation, generation, proof, retained, remembered) = committed;

        let context = self
            .publication_context(operation, Some(generation), proof, accounting)
            .await?;
        *acknowledged = Some(crate::NativePublicationSnapshot {
            context,
            operation: Some(operation),
            generation,
        });
        if retained == Some(operation) {
            self.authored
                .shared_checkout()
                .publish(
                    permit,
                    crate::kernel::KernelAccounting::native(work, cancellation),
                    Some(self.authored.cancellation()),
                    before_publication,
                    Some(context),
                    acknowledged,
                )
                .await
                .map_err(|(error, accounting)| {
                    *accounting_failure = accounting;
                    error
                })?;
            if !acknowledged.is_some_and(|snapshot| {
                snapshot.operation() == Some(operation) && snapshot.generation() == generation
            }) {
                return Err(MountSourceError::Indeterminate.into());
            }
        }
        self.finish_publication(operation, generation, proof, accounting)
            .await?;
        if remembered == Some(operation) {
            self.finish_detached_publication(work)?;
            self.clear_mount_removals()?;
        }
        Ok(crate::NativePublicationSnapshot {
            context,
            operation: Some(operation),
            generation,
        })
    }
    async fn recover_retained_publication<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        before_publication: &mut F,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        remembered: (crate::OperationId, Option<SelectedImportProof>),
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { .. } = &*accounting else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (operation, proof) = remembered;

        let context = self
            .publication_context(operation, None, proof, accounting)
            .await?;
        self.complete_publication(
            permit,
            (context, operation, proof),
            accounting,
            before_publication,
            acknowledged,
            accounting_failure,
        )
        .await
    }
    pub(super) async fn cleanup_publication(
        &self,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError> {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let retained = adapter::await_lock(
            self.authored.shared_checkout().lock(),
            &mut carrier,
            cancellation,
            Some(self.authored.cancellation()),
        )
        .await?
        .has_retained_operation();
        let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
        if !retained
            && self
                .lazy
                .mount_removal_publication(&mut accounting)
                .await
                .map_err(lazy_error)?
                == Some(false)
        {
            self.lazy
                .finish_mount_removals(&mut accounting)
                .await
                .map_err(lazy_error)?;
            self.abort_detached_publication()?;
            self.removals
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .publication = None;
        }
        Ok(())
    }
}
