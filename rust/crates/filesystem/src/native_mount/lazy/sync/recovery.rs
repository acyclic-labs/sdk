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
        publication: &mut PublicationState<'_, F>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        selection: (SelectedPublication<'_>, Option<SelectedImportProof>),
    ) -> Result<Option<crate::NativePublicationSnapshot>, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let (work, cancellation) = (publication.work, publication.cancellation);
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
                    publication,
                    accounting,
                    (
                        operation,
                        generation,
                        proof,
                        retained,
                        remembered.map(|(id, _)| id),
                    ),
                )
                .await
                .map(Some);
        }
        if let Some(operation) = operation {
            if retained == Some(operation) {
                return self
                    .recover_retained_publication(
                        publication,
                        accounting,
                        (operation, remembered.and_then(|(_, proof)| proof)),
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
        publication: &mut PublicationState<'_, F>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        committed: CommittedPublication,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let (operation, generation, proof, retained, remembered) = committed;

        let context = self
            .publication_context(operation, Some(generation), proof, accounting)
            .await?;
        *publication.acknowledged = Some(crate::NativePublicationSnapshot {
            context,
            operation: Some(operation),
            generation,
        });
        if retained == Some(operation) {
            let published = self.publish_checkout(publication, context).await?;
            if published.operation() != Some(operation) || published.generation() != generation {
                return Err(MountSourceError::Indeterminate.into());
            }
        }
        self.finish_publication(operation, generation, proof, accounting)
            .await?;
        if remembered == Some(operation) {
            self.finish_detached_publication(publication.work)?;
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
        publication: &mut PublicationState<'_, F>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        remembered: (crate::OperationId, Option<SelectedImportProof>),
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let (operation, proof) = remembered;

        let context = self
            .publication_context(operation, None, proof, accounting)
            .await?;
        self.complete_publication(publication, (context, operation, proof), accounting)
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
