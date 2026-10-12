use super::*;
use std::future::Future;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError> {
        self.sync_async_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Publishes pending authored writes under one authority permit.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError> {
        let work = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED,
            self.authored.cancellation(),
        )
        .map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        self.sync_async_with_permit_qualified_native_work_guarded_with_ack(
            permit,
            &work,
            work.cancellation(),
            &mut None,
            self.authored.sdk_publication_policy(),
        )
        .await
        .map(drop)
        .map_err(|(error, _)| error)
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError> {
        self.runtime.wait(|| self.sync_async())
    }

    pub(in crate::native_mount) async fn sync_async_with_permit_qualified_native_work_guarded_with_ack<
        F,
        Fut,
        E,
    >(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        mut before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let _phase = self
            .source_view
            .write_stable_with_carrier(
                None,
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await
            .map_err(|error| (E::from(error), None))?
            .into_mutation_phase();
        self.publish(
            permit,
            work,
            cancellation,
            acknowledged,
            &mut before_publication,
            None,
        )
        .await
    }

    pub(in crate::native_mount) async fn import_selected<F, Fut, E>(
        &self,
        destination: crate::LazySnapshotRef,
        plan: crate::SelectedImportPlan,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        mut carrier: adapter::NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> Result<crate::LazySnapshotRef, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::OriginalSnapshotUnavailable)?;
        let _phase = self
            .source_view
            .write_stable_with_carrier(
                None,
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await?
            .into_mutation_phase();
        let mut accounting = crate::kernel::KernelAccounting::native(&work, cancellation);
        if let Some(generation) = self
            .lazy
            .operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
                &mut accounting,
            )
            .await
            .map_err(lazy_error)?
        {
            let input = plan
                .input_digest(destination, &mut accounting)
                .map_err(lazy_error)?;
            let remembered = self
                .removals
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .publication;
            let retained = {
                let mut checkout = self
                    .authored
                    .shared_checkout()
                    .lock_with_carrier(
                        &mut carrier,
                        cancellation,
                        Some(self.authored.cancellation()),
                    )
                    .await?;
                checkout
                    .has_retained_operation()
                    .then(|| checkout.retained_operation_id())
            };
            if retained == Some(operation)
                && !self
                    .authored
                    .shared_checkout()
                    .resolve_acknowledged_original_with_native_work(
                        operation,
                        generation,
                        &work,
                        cancellation,
                        Some(self.authored.cancellation()),
                        self.authored.publication_guard(),
                    )
                    .await?
            {
                return Err(MountSourceError::Indeterminate.into());
            }
            let snapshot = self
                .lazy
                .finish_selected_import(
                    operation,
                    generation,
                    destination,
                    plan.source,
                    plan.source_config,
                    input,
                    &mut accounting,
                )
                .await
                .map_err(lazy_error)?;
            if remembered.is_some_and(|(id, _)| id == operation) {
                self.finish_detached_publication()?;
                self.clear_mount_removals()?;
            }
            return Ok(snapshot);
        }
        let published = self
            .publish(
                permit,
                &work,
                cancellation,
                &mut None,
                &mut before_publication,
                Some((destination, &plan, operation)),
            )
            .await
            .map_err(|(error, _)| error)?;
        published
            .lazy_snapshot(&mut crate::kernel::KernelAccounting::native(
                &work,
                cancellation,
            ))
            .map_err(mount_engine_error)?
            .ok_or_else(|| E::from(MountSourceError::OriginalSnapshotUnavailable))
    }

    pub(super) async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.publish(
            permit,
            work,
            work.cancellation(),
            &mut None,
            &mut self.authored.sdk_publication_policy(),
            None,
        )
        .await
        .map(drop)
        .map_err(|(error, _)| error)
    }
}
