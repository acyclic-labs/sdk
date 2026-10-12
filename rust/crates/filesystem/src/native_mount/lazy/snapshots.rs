use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(in crate::native_mount) async fn snapshot(
        &self,
        carrier: adapter::NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::LazySnapshotRef, MountSourceError> {
        let original = carrier
            .original_native_work()
            .ok_or(MountSourceError::OriginalSnapshotUnavailable)?;
        self.publication_snapshot(carrier, cancellation)
            .await
            .map_err(|(error, _)| error)?
            .lazy_snapshot(&mut crate::kernel::KernelAccounting::native(
                &original,
                cancellation,
            ))
            .map_err(mount_engine_error)?
            .ok_or(MountSourceError::OriginalSnapshotUnavailable)
    }

    pub(in crate::native_mount) async fn publication_snapshot(
        &self,
        mut carrier: adapter::NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::NativePublicationSnapshot, (MountSourceError, Option<MountSourceError>)>
    {
        let result = async {
            let original = carrier
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
            let mut scratch_carrier = adapter::NativeDemandCarrier::Native(&original);
            let mut scratch = adapter::InputBuffers::new(&mut scratch_carrier)?;
            let detached = {
                let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                if !removals.paths.is_empty() || removals.publication.is_some() {
                    return Err(MountSourceError::Stale);
                }
                let identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                let mut files = scratch.vec(identities.len())?;
                for entry in identities.values() {
                    scratch.items(1)?;
                    if !entry.ready || entry.pending_epoch.is_some() {
                        return Err(MountSourceError::Stale);
                    }
                    files.push((Arc::clone(&entry.file), entry.published_epoch));
                }
                files
            };
            for (file, published) in &detached {
                if file.snapshot(&mut carrier, cancellation).await?.2 != *published {
                    return Err(MountSourceError::Stale);
                }
            }
            let checkout = adapter::await_lock(
                self.authored.shared_checkout().lock(),
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations() || self.authored.captures_pending() {
                return Err(MountSourceError::Stale);
            }
            let generation = checkout.generation_id();
            drop(checkout);
            let mut accounting = crate::kernel::KernelAccounting::native(&original, cancellation);
            let snapshot = self
                .lazy
                .publication_snapshot(&mut accounting)
                .await
                .map_err(lazy_error)?;
            let frozen = snapshot
                .lazy_snapshot(&mut accounting)
                .map_err(mount_engine_error)?
                .ok_or(MountSourceError::OriginalSnapshotUnavailable)?;
            let checkout = adapter::await_lock(
                self.authored.shared_checkout().lock(),
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations()
                || self.authored.captures_pending()
                || checkout.generation_id() != generation
                || frozen.authored_generation != generation
            {
                return Err(MountSourceError::Stale);
            }
            Ok(snapshot)
        }
        .await;
        let accounting = carrier
            .with_work(|work| work.record(crate::WorkCounters::UNCHARGED))
            .map_err(mount_demand_error);
        match (result, accounting) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Ok(_), Err(error)) => Err((error, None)),
            (Err(error), Ok(())) => Err((error, None)),
            (Err(error), Err(accounting)) => Err((error, Some(accounting))),
        }
    }

    pub(crate) async fn selected_import_snapshot(
        &self,
        operation: crate::OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::LazySnapshotRef>, MountSourceError> {
        let mut accounting = crate::kernel::KernelAccounting::native(original, cancellation);
        let Some(generation) = self
            .lazy
            .operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
                &mut accounting,
            )
            .await
            .map_err(lazy_error)?
        else {
            return Ok(None);
        };
        let mut carrier = adapter::NativeDemandCarrier::Native(original);
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
        let (snapshot, _) = self
            .lazy
            .selected_snapshot(operation, generation, &mut accounting)
            .await
            .map_err(lazy_error)?;
        let remembered = self
            .removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .publication;
        if let Some((id, Some((destination, source, config, input)))) = remembered
            && id == operation
        {
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
            if retained == Some(operation)
                && !self
                    .authored
                    .shared_checkout()
                    .recover_publication(
                        operation,
                        generation,
                        original,
                        cancellation,
                        Some(self.authored.cancellation()),
                        self.authored.callback_guard().cloned(),
                    )
                    .await?
            {
                return Err(MountSourceError::Indeterminate);
            }
            self.lazy
                .finish_selected_import(
                    operation,
                    generation,
                    destination,
                    source,
                    config,
                    input,
                    &mut accounting,
                )
                .await
                .map_err(lazy_error)?;
            self.finish_detached_publication(original)?;
            self.clear_mount_removals()?;
        }
        Ok(Some(snapshot))
    }
}
