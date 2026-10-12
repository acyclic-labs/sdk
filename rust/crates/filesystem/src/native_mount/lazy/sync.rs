use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?.into_mutation_phase();
        self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

/// Publishes the mounted candidate and removal journal with bounded work.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = {
            let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut work);
            let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
            match scratch.prepare_finalization() {
                Err(error) => Err(error),
                Ok(()) => {
                    let mut before_publication = self.authored.sdk_publication_policy();
                    let result = self.sync_async_with_permit_carrier_guarded(
                        permit, scratch.carrier.reborrow(), cancellation, &mut before_publication,
                    ).await;
                    let finalized = scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED));
                    match (result, finalized) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Err(primary), Ok(())) => Err(primary),
                        (Ok(()), Err(cleanup)) => Err(mount_demand_error(cleanup)),
                        (Err(primary), Err(cleanup)) => Err(scratch.finalization_error(primary, cleanup)),
                    }
                }
            }
        };
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }

pub(in crate::native_mount::lazy) async fn sync_async_with_permit_carrier_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit,
        carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>, cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        work.check().map_err(mount_demand_error)?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        self.sync_staged(permit, &mut work, cancellation, before_publication, None).await.map(|_| ())
    }

/// Verifies the caller's original owner immediately before every SDK seal.
    pub async fn sync_async_with_permit_measured_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, (E, Option<MountSourceError>)>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = async {
            crate::native_mount::adapter::BorrowedNativeDemandMeter::new(&mut work).map_err(E::from)?
                .run_async(|meter| async move {
                    self.sync_async_with_permit_carrier_guarded(
                        permit, crate::native_mount::adapter::NativeDemandCarrier::Shared(&meter), cancellation, &mut before_publication,
                    ).await
                }).await
        }.await;
        let finalized = work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error);
        match (result, finalized) {
            (Ok(value), Ok(())) => Ok(crate::OperationReceipt { value, work: work.work }),
            (Err(primary), Ok(())) => Err(crate::OperationFailure::new((primary, None), work.work)),
            (Ok(()), Err(cleanup)) => Err(crate::OperationFailure::new((E::from(cleanup), None), work.work)),
            (Err(primary), Err(cleanup)) => Err(crate::OperationFailure::new((primary, Some(cleanup)), work.work)),
        }
    }

/// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let _mutation = self.mutation_lease(None)?;
        self.wait(|| async {
            self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
                .await
        })
    }

pub(in crate::native_mount::lazy) async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        self.sync_staged(
            permit, &mut work, self.authored.cancellation(),
            &mut self.authored.sdk_publication_policy(), None,
        ).await.map(|_| ())
    }

pub(super) async fn sync_staged<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
        selected: Option<SelectedMountInput<'_>>,
    ) -> Result<Option<crate::LazySnapshotRef>, E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let selected_proof = if let Some(input) = &selected {
            let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(input.operation.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?;
            let proof = SelectedImportProof {
                checkpoint: input.destination, source: input.plan.source,
                source_config: input.plan.source_config,
                input: input.plan.input_digest(
                    input.destination, &LazyTreapWork::native_with_work(
                        input.original, cancellation,
                    ).map_err(native_lazy_error)?,
                ).map_err(native_lazy_error)?,
            };
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            if let Some(custody) = custody
                && (custody.operation != input.operation || custody.selected != Some(proof))
            {
                return Err(E::from(MountSourceError::Stale));
            }
            if let Some(generation) = generation {
                if custody.is_some() {
                    return self.finish_acknowledged_mount_custody(
                        input.operation, generation, work, input.original, cancellation,
                    ).await.map_err(E::from);
                }
                return self.lazy.selected_committed_snapshot(input.operation, generation, proof.checkpoint, proof.source, proof.source_config, proof.input, &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map(Some).map_err(native_lazy_error).map_err(E::from);
            }
            if custody.is_some() {
                self.recover_pending_mount_change_measured(
                    permit, work, cancellation, before_publication,
                ).await?;
                let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(input.operation.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
                return self.finish_acknowledged_mount_custody(
                    input.operation, generation, work, input.original, cancellation,
                ).await.map_err(E::from);
            }
            Some(proof)
        } else { None };
        self.recover_pending_mount_change_measured(
            permit, work, cancellation, before_publication,
        ).await?;
        let inputs = self.snapshot_sync_inputs(work, cancellation.clone()).await?;
        if selected.is_none() && inputs.paths.value.is_empty() && inputs.dirty.value.is_empty() {
            if work.observation == MountPublicationObservation::Qualified {
                let original = work.meter.original_native_work().ok_or(MountSourceError::Indeterminate)?;
                let context = crate::lazy_workspace::NativeSnapshotContext::Lazy(
                    self.lazy.publication_cut(None, &original, cancellation)
                        .await.map_err(native_lazy_error)?,
                );
                work.sync_authored_context_guarded(
                    &self.authored, permit, false, cancellation, before_publication, context,
                ).await?;
            } else {
                work.sync_authored_guarded(
                    &self.authored, permit, false, cancellation, before_publication,
                ).await?;
            }
            self.prune_detached(work)?;
            return Ok(None);
        }
        let PreparedMountSync { mut candidate, epoch, stamp, inputs, mut records } =
            self.prepare_sync_candidate_from_inputs(inputs, work, cancellation).await?;
        let mut effects = if let Some(input) = &selected {
            let effects = (async { let accounting = LazyTreapWork::native_with_work(input.original, cancellation)?; self.lazy.prepare_selected_import_into_checkout(
                input.destination, input.plan, &mut candidate.checkout, input.operation, &accounting,
            ).await }).await.map_err(native_lazy_error)?;
            let original_records = records.value.iter().map(|(record, metadata)| (record, metadata));
            let selected_records = effects.records.iter().map(|(record, metadata)| (record, metadata));
            let count = mount_record_union(
                original_records.clone(), selected_records.clone(), std::iter::empty(), None, &mut work.meter,
            )?;
            let mut merged = work.owned_vec(count)?;
            mount_record_union(
                original_records, selected_records, std::iter::empty(), Some(&mut merged.value), &mut work.meter,
            )?;
            records = merged;
            Some(effects)
        } else { None };
        let paths = &inputs.paths.value;
        let dirty = &inputs.dirty.value;
        let operation_id = {
            let config = candidate.checkout.volume_config();
            let mut removed = work.owned_vec(paths.len())?;
            for path in paths {
                let mut namespace = work.namespace_buffer(path, config)?;
                removed.absorb_buffer_guard(&mut namespace)?;
                removed.value.push(namespace.value.take().expect("prepared removal namespace"));
            }
            let scope = effects.as_ref().map_or(InstallScope::Paths(&removed.value), |effects|
                InstallScope::CombinedPaths(&removed.value, &effects.changes));
            let (installed, allocation) = crate::native_mount::adapter::installed_changes(
                &candidate.base, &candidate.checkout, scope, &mut work.meter, cancellation,
            ).await?;
            let installed = if let Some(allocation) = allocation {
                let bytes = allocation.bytes();
                MountSyncBuffer { value: Some(installed), custody: None, bytes, native_allocation: Some(allocation) }
            } else {
                let installed_bytes = mount_effect_bytes(&installed);
                work.adopt(installed_bytes).map_err(mount_demand_error)?;
                retain_sync_buffer(Some(installed), installed_bytes, work)?
            };
            self.stage_detached_records_measured(&records.value, dirty, work, cancellation).await?;
            let custody_items = self.removals.lock().map_err(|_| MountSourceError::Stale)?.paths.len()
                .checked_add(self.detached.lock().map_err(|_| MountSourceError::Stale)?.len())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            let custody_work = crate::WorkCounters { items_examined: custody_items, ..crate::WorkCounters::default() };
            work.reserve_items(custody_items)?;
            let record = |carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>| {
                crate::native_mount::adapter::record_view_change(&self.authored.shared_checkout().ledger,
                    &ViewChange::Effect(installed.value.as_ref().expect("original mounted effects")), carrier,
                )
            };
            let installed_result = self.authored.shared_checkout().finish_phase(
                candidate, epoch, stamp, &mut work.meter, cancellation,
                Some(self.authored.cancellation()), record, selected.as_ref().map(|input| input.operation),
                |publication, state| (publication, state.retained_operation_id()),
            ).await;
            work.restore_items(custody_items)?;
            let operation = match installed_result {
                Ok(Some((_, operation))) => operation,
                Ok(None) => return Err(E::from(MountSourceError::Stale)),
                Err(error) => return Err(E::from(error)),
            };
            let displaced = {
                let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                for removed in removals.paths.values_mut() { removed.operation = Some(operation); }
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                for (_, entry) in identities.iter_mut() {
                    if let Some(pending) = &mut entry.pending { pending.operation = Some(operation); }
                }
                removals.publication = Some(MountedPublicationCustody {
                    operation, published: false, selected: selected_proof,
                });
                std::mem::replace(&mut removals.selected_inputs, effects.take())
            };
            drop(displaced);
            work.record(custody_work).map_err(mount_demand_error)?;
            drop(removed);
            drop(installed);
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            operation
        };
        let native_owned = match &work.meter {
            crate::native_mount::adapter::NativeDemandCarrier::Shared(meter) =>
                Some(crate::NativeMaterializationWork::from_meter(Arc::clone(meter))),
            crate::native_mount::adapter::NativeDemandCarrier::Borrowed(_)
                | crate::native_mount::adapter::NativeDemandCarrier::Native(_) => None,
        };
        let native = match &work.meter {
            crate::native_mount::adapter::NativeDemandCarrier::Native(original) => Some(*original),
            _ => native_owned.as_ref(),
        };
        let selected_inputs = SelectedInputsLoan::take(&self.removals, operation_id, work)?;
        if let Some(original) = native {
            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.prepare_mount_removals(paths, selected_inputs.value.as_ref().map_or(&[], |effects| effects.paths.as_slice()), &records.value, IdempotencyKey::from_bytes(operation_id.into_bytes()), &accounting).await }).await.map_err(native_lazy_error)?;
        } else {
            let (budget, prior_live) = work.sdk_scope()?;
            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.prepare_mount_removals(paths, &[], &records.value, IdempotencyKey::from_bytes(operation_id.into_bytes()), &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
            work.accept_sdk(result, prior_live, lazy_error)?;
        }
        selected_inputs.discard();
        drop(inputs);
        drop(records);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        if work.observation == MountPublicationObservation::Qualified {
            let original = work.meter.original_native_work().ok_or(MountSourceError::Indeterminate)?;
            let context = crate::lazy_workspace::NativeSnapshotContext::Lazy(
                self.lazy.publication_cut(Some(operation_id), &original, cancellation)
                    .await.map_err(native_lazy_error)?,
            );
            work.sync_authored_context_guarded(
                &self.authored, permit, true, cancellation, before_publication, context,
            ).await?;
        } else {
            work.sync_authored_guarded(
                &self.authored, permit, true, cancellation, before_publication,
            ).await?;
        }
        self.mark_mount_published(operation_id)?;
        if let Some(input) = &selected {
            let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(operation_id.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
            return self.finish_acknowledged_mount_custody(
                operation_id, generation, work, input.original, cancellation,
            ).await.map_err(E::from);
        }
        let finished = if let Some(original) = native {
            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
        } else {
            let (budget, prior_live) = work.sdk_scope()?;
            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
            work.accept_sdk(result, prior_live, lazy_error)?
        };
        if !finished { return Err(E::from(MountSourceError::Stale)); }
        self.finish_mount_custody(operation_id, work)?;
        Ok(None)
    }
}
