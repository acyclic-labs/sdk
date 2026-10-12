use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount) async fn publication_snapshot(
        &self, carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::NativePublicationSnapshot,
        (MountSourceError, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let (_, qualified) = self.capture_snapshot(
            carrier, cancellation, MountPublicationObservation::Qualified,
        ).await?;
        qualified.ok_or((MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        ), None))
    }

/// Reads the actual mounted lazy identity.
    pub(in crate::native_mount) async fn snapshot(
        &self, carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::LazySnapshotRef, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.capture_snapshot(
            carrier, cancellation, MountPublicationObservation::Ordinary,
        ).await.map(|(snapshot, _)| snapshot).map_err(|(primary, _)| primary)
    }

pub(in crate::native_mount::lazy) async fn capture_snapshot(
        &self, carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, policy: MountPublicationObservation,
    ) -> Result<(crate::LazySnapshotRef, Option<crate::NativePublicationSnapshot>),
        (MountSourceError, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut work = MountSyncWork::new(&self.authored, carrier)
            .map_err(|primary| (primary, None))?;
        let result = async {
            work.check().map_err(mount_demand_error)?;
            let _mutation = self.source_view.write_stable_with_carrier(
                None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
            ).await?.into_mutation_phase();
            {
                let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                work.items(1)?;
                if !removals.paths.is_empty() || removals.publication.is_some() {
                    return Err(MountSourceError::Stale);
                }
            }
            let detached = {
                let identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                let mut detached = work.owned_vec(identities.len())?;
                for (_, entry) in identities.iter() {
                    work.items(1)?;
                    if !entry.ready || entry.pending.is_some() { return Err(MountSourceError::Stale); }
                    detached.push((Arc::clone(&entry.file), entry.published_epoch));
                }
                detached
            };
            for (file, published) in &detached.value {
                work.items(1)?;
                if file.snapshot(&mut work.meter, cancellation).await?.2 != *published {
                    return Err(MountSourceError::Stale);
                }
            }
            let checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations() || self.authored.captures_pending() {
                return Err(MountSourceError::Stale);
            }
            let generation = checkout.generation_id();
            drop(checkout);
            let mut qualified = None;
            let snapshot = if let Some(original) = work.meter.original_native_work() {
                if policy == MountPublicationObservation::Qualified {
                    let publication = self.lazy.publication_snapshot_with_native_work(&original)
                        .await.map_err(native_lazy_error)?;
                    let snapshot = publication.snapshot()
                        .lazy_snapshot_with_native_work(&original, cancellation)
                        .map_err(mount_engine_error)?
                        .ok_or(MountSourceError::Indeterminate)?;
                    qualified = Some(publication);
                    snapshot
                } else {
                    self.lazy.snapshot(&LazyTreapWork::native_with_work(&original, original.cancellation())
                        .map_err(native_lazy_error)?)
                        .await.map_err(native_lazy_error)?
                }
            } else {
                if policy == MountPublicationObservation::Qualified {
                    return Err(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::MissingCustody,
                    ));
                }
                let prior_live = work.with_work(|meter| meter.live_allocation_bytes());
                let accounting = LazyTreapWork::new(work.remaining().map_err(mount_demand_error)?, cancellation);
                let result = self.lazy.snapshot(&accounting).await;
                let receipt = accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure());
                work.meter.accept_with_prior_live(receipt, prior_live, lazy_error)?
            };
            let checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations() || self.authored.captures_pending()
                || checkout.generation_id() != generation
                || snapshot.authored_generation != generation {
                return Err(MountSourceError::Stale);
            }
            Ok((snapshot, qualified))
        }.await;
        let accounting = if policy == MountPublicationObservation::Qualified || result.is_ok() {
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)
        } else {
            Ok(())
        };
        match (result, accounting) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Ok(snapshot), Err(error)) => {
                drop(snapshot);
                Err((error, None))
            }
            (Err(primary), Ok(())) => Err((primary, None)),
            (Err(primary), Err(accounting)) => Err((primary, Some(accounting))),
        }
    }

pub(crate) async fn sync_async_with_permit_qualified_native_work_guarded_with_ack<F, Fut, E>(
        &self, permit: crate::PublicationPermit, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>, mut before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = MountSyncWork::new(
            &self.authored, crate::native_mount::adapter::NativeDemandCarrier::Native(original),
        ).map_err(|error| (E::from(error), None))?;
        work.observation = MountPublicationObservation::Qualified;
        work.check().map_err(|error| (E::from(mount_demand_error(error)), None))?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await.map_err(|error| (E::from(error), None))?.into_mutation_phase();
        let result = self.sync_staged(
            permit, &mut work, cancellation, &mut before_publication, None,
        ).await;
        if let Some(snapshot) = work.publication_snapshot.filter(|snapshot| snapshot.operation().is_some()) {
            *acknowledged = Some(snapshot);
        }
        result.map_err(|primary| (primary, work.accounting_failure.take()))?;
        work.publication_snapshot.ok_or_else(|| (E::from(MountSourceError::Indeterminate), None))
    }

pub(in crate::native_mount) async fn import_selected<F, Fut, E>(
        &self, destination: crate::LazySnapshotRef, plan: crate::SelectedImportPlan,
        operation: crate::OperationId, permit: crate::PublicationPermit,
        carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, mut before_publication: F,
    ) -> Result<crate::LazySnapshotRef, E>
    where
        A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let original = carrier.original_native_work().ok_or_else(|| E::from(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        )))?;
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        work.check().map_err(mount_demand_error)?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        self.sync_staged(
            permit, &mut work, cancellation, &mut before_publication,
            Some(SelectedMountInput { destination, plan: &plan, operation, original: &original }),
        ).await?.ok_or_else(|| E::from(MountSourceError::OriginalSnapshotUnavailable))
    }

/// Reads the acknowledged selected publication.
    pub(crate) async fn selected_import_snapshot(
        &self,
        operation: crate::OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::LazySnapshotRef>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let Some(generation) = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)? else { return Ok(None); };
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(original);
        let _stable = self.source_view.write_stable_with_carrier(
            None, &mut carrier, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        self.finish_acknowledged_mount_custody(
            operation, generation, &mut work, original, cancellation,
        ).await?.map(Some).ok_or(MountSourceError::Indeterminate)
    }
}
