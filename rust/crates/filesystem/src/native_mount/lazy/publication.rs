use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn finish_detached_publication(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let count = self.detached.lock().map_err(|_| MountSourceError::Stale)?.len();
        for position in 0..count {
            let (file_id, epoch, notify) = {
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                work.items(1)?;
                let (file_id, entry) = &mut identities[position];
                let Some(pending) = entry.pending.as_ref().filter(|pending| pending.operation == Some(operation)) else { continue; };
                if entry.ready {
                    entry.published_epoch = pending.epoch;
                    continue;
                }
                (*file_id, pending.epoch, !entry.ready)
            };
            work.reserve_items(1)?;
            if notify {
                crate::native_mount::adapter::record_view_change(&self.authored.shared_checkout().ledger,
                    &ViewChange::Node(file_id), &mut work.meter,
                )?;
            }
            let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let (current_id, entry) = identities.get_mut(position).ok_or(MountSourceError::Stale)?;
            work.complete_items(1)?;
            if *current_id != file_id || !entry.pending.as_ref().is_some_and(|pending|
                pending.operation == Some(operation) && pending.epoch == epoch)
            {
                return Err(MountSourceError::Stale);
            }
            entry.ready = true;
            entry.published_epoch = epoch;
        }
        Ok(())
    }

pub(in crate::native_mount::lazy) fn prune_detached(&self, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        self.prune_detached_operation(None, work)
    }

pub(in crate::native_mount::lazy) fn prune_detached_operation(
        &self,
        operation: Option<crate::OperationId>,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let mut open = self.open_sources.lock().map_err(|_| MountSourceError::Stale)?;
        let mut handle_items = 0_u64;
        for handles in open.values() {
            work.items(1)?;
            handle_items = handle_items.checked_add(handles.len() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let cleanup = crate::WorkCounters {
            items_examined: handle_items.checked_add(identities.len() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
            ..crate::WorkCounters::default()
        };
        work.check().map_err(mount_demand_error)?;
        work.admit(&cleanup)?;
        open.retain(|_, handles| {
            handles.retain(|handle| handle.strong_count() != 0);
            !handles.is_empty()
        });
        identities.retain_mut(|(id, entry)| {
            if operation.is_some() && entry.pending.as_ref().is_some_and(|pending| pending.operation == operation) {
                entry.pending = None;
            }
            removals.records.contains_key(id)
                || open.contains_key(id)
                || !entry.ready
                || entry.pending.is_some()
                || Arc::strong_count(&entry.file) != 1
                || entry.file.mutation_epoch_if_idle().map_or(true, |epoch| epoch > entry.published_epoch)
        });
        work.record(cleanup).map_err(mount_demand_error)?;
        Ok(())
    }

pub(in crate::native_mount::lazy) fn abort_detached_publication(&self, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(identities.len())?;
        identities.retain_mut(|(_, entry)| {
            if entry.pending.as_ref().is_some_and(|pending| pending.operation.is_none()) {
                entry.pending = None;
            }
            entry.ready || entry.pending.is_some()
        });
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(())
    }

pub(in crate::native_mount::lazy) async fn stage_detached_records_measured(
        &self,
        records: &[(FileRecord, FileMetadata)],
        dirty: &[DirtyDetachedRecord],
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        for (record, metadata) in records {
            work.items(1)?;
            let position = self.reserve_detached_slot_with_carrier(record.file_id, &mut work.meter)?;
            if let Some(position) = position {
                let file = self.authored.detached_file_from_record(
                    *record, *metadata, SourceViewGate::callback_owner(), &mut work.meter,
                ).await?;
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                work.charge(crate::WorkCounters {
                    bytes_copied: ((identities.len() - position) * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
                    ..crate::WorkCounters::default()
                }).map_err(mount_demand_error)?;
                identities.insert(position, (record.file_id, DetachedIdentity {
                    file, ready: false, published_epoch: 0, pending: None,
                }));
            }
        }
        let mut dirty_position = 0_usize;
        for (record, metadata) in records {
            work.items(1)?;
            let dirty_epoch = if let Some((id, _, _, epoch)) = dirty.get(dirty_position) {
                work.items(1)?;
                if *id == record.file_id { dirty_position += 1; Some(*epoch) } else { None }
            } else { None };
            let frozen = self.authored.detached_identity_from_record_with_carrier(
                *record, SourceViewGate::callback_owner(), &mut work.meter, cancellation,
            ).await?;
            let mut pending = work.with_work(|meter| retain_pending_detached_with_work(
                frozen, *metadata, dirty_epoch.unwrap_or(0), None, meter,
            ))?;
            let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let position = work.with_work(|meter| detached_position(&identities, record.file_id, meter))?
                .map_err(|_| MountSourceError::Stale)?;
            let entry = &mut identities.get_mut(position).ok_or(MountSourceError::Stale)?.1;
            if entry.pending.is_some() { return Err(MountSourceError::Stale); }
            if dirty_epoch.is_none() { pending.epoch = entry.published_epoch; }
            entry.pending = Some(pending);
        }
        Ok(())
    }

#[cfg(test)]
    pub(in crate::native_mount::lazy) async fn recover_pending_mount_change(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        self.recover_pending_mount_change_measured(
            permit, &mut work, self.authored.cancellation(),
            &mut self.authored.sdk_publication_policy(),
        ).await
    }

pub(in crate::native_mount::lazy) fn recover_pending_mount_change_measured<'a, 'work, F, Fut, E>(
        &'a self,
        permit: crate::PublicationPermit,
        work: &'a mut MountSyncWork<'work>,
        cancellation: &'a crate::CancellationToken,
        before_publication: &'a mut F,
    ) -> impl Future<Output = Result<(), E>> + Send + 'a
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        async move {
            let native = match &work.meter {
                crate::native_mount::adapter::NativeDemandCarrier::Shared(meter) =>
                    Some(crate::NativeMaterializationWork::from_meter(Arc::clone(meter))),
                crate::native_mount::adapter::NativeDemandCarrier::Native(original) => Some((*original).clone()),
                crate::native_mount::adapter::NativeDemandCarrier::Borrowed(_) => None,
            };
            work.items(1)?;
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            if let (Some(custody), Some(original)) = (custody, native.as_ref())
                && custody.selected.is_some()
                && let Some(generation) = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(custody.operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?
            {
                return self.finish_acknowledged_mount_custody(
                    custody.operation, generation, work, original, cancellation,
                ).await?.ok_or(MountSourceError::Indeterminate).map(|_| ()).map_err(E::from);
            }
            let mut committed = if let Some(original) = native.as_ref() {
                (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.mount_removal_publication(&accounting).await }).await.map_err(native_lazy_error)?
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                work.accept_sdk(result, prior_live, lazy_error)?
            };
            if let Some(custody) = custody {
                if !custody.published {
                    if committed.is_none() {
                        let (paths, records) = self.frozen_mount_inputs(custody.operation, work)?;
                        let selected_inputs = SelectedInputsLoan::take(&self.removals, custody.operation, work)?;
                        if let Some(original) = native.as_ref() {
                            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.prepare_mount_removals(&paths.value, selected_inputs.value.as_ref().map_or(&[], |effects| effects.paths.as_slice()), &records.value, IdempotencyKey::from_bytes(custody.operation.into_bytes()), &accounting).await }).await.map_err(native_lazy_error)?;
                        } else {
                            let (budget, prior_live) = work.sdk_scope()?;
                            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.prepare_mount_removals(&paths.value, &[], &records.value, IdempotencyKey::from_bytes(custody.operation.into_bytes()), &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                            work.accept_sdk(result, prior_live, lazy_error)?;
                        }
                        selected_inputs.discard();
                        drop(paths);
                        drop(records);
                        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
                        committed = Some(false);
                    }
                    let retained = {
                        let mut checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
                        checkout.has_retained_operation().then(|| checkout.retained_operation_id())
                    };
                    if retained == Some(custody.operation) {
                        work.sync_authored_recovery_guarded(
                            &self.authored, permit, cancellation.clone(), before_publication,
                        ).await?;
                    } else if committed != Some(true) {
                        return Err(E::from(MountSourceError::Stale));
                    }
                    self.mark_mount_published(custody.operation)?;
                }
                if let Some(original) = native.as_ref()
                    && custody.selected.is_some()
                {
                    let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(custody.operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
                    return self.finish_acknowledged_mount_custody(
                        custody.operation, generation, work, original, cancellation,
                    ).await?.ok_or(MountSourceError::Indeterminate).map(|_| ()).map_err(E::from);
                }
                if committed.is_some() {
                    let finished = if let Some(original) = native.as_ref() {
                        (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
                    } else {
                        let (budget, prior_live) = work.sdk_scope()?;
                        let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                        work.accept_sdk(result, prior_live, lazy_error)?
                    };
                    if !finished { return Err(E::from(MountSourceError::Stale)); }
                }
                return self.finish_mount_custody(custody.operation, work).map_err(E::from);
            }
            let retained = work.lock_checkout(&self.authored, cancellation.clone()).await?.has_retained_operation();
            if retained {
                work.sync_authored_recovery_guarded(
                    &self.authored, permit, cancellation.clone(), before_publication,
                ).await?;
            }
            if let Some(committed) = committed {
                let published = if let Some(original) = native.as_ref() {
                    (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
                } else {
                    let (budget, prior_live) = work.sdk_scope()?;
                    let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                    work.accept_sdk(result, prior_live, lazy_error)?
                };
                if (committed || retained) && !published { return Err(E::from(MountSourceError::Stale)); }
            }
            self.abort_detached_publication(work).map_err(E::from)
        }
    }

pub(in crate::native_mount::lazy) fn frozen_mount_inputs(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(MountSyncBuffer<Vec<String>>, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let paths = {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            work.items(removals.paths.len())?;
            let count = removals.paths.values().filter(|removed| removed.operation == Some(operation)).count();
            let mut paths = work.owned_vec::<String>(count)?;
            for (path, removed) in &removals.paths {
                work.items(1)?;
                if removed.operation != Some(operation) { continue; }
                paths.push_cloned_text(&path.text, work)?;
            }
            paths
        };
        let identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(identities.len())?;
        let count = identities.iter().filter(|(_, entry)|
            entry.pending.as_ref().is_some_and(|pending| pending.operation == Some(operation))).count();
        let mut records = work.owned_vec(count)?;
        for (_, entry) in identities.iter() {
            work.items(1)?;
            if let Some(pending) = entry.pending.as_ref().filter(|pending| pending.operation == Some(operation)) {
                work.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<(FileRecord, FileMetadata)>() as u64,
                    ..crate::WorkCounters::default()
                }).map_err(mount_demand_error)?;
                records.push((pending.file.record(), pending.metadata));
            }
        }
        Ok((paths, records))
    }

pub(in crate::native_mount::lazy) fn mark_mount_published(&self, operation: crate::OperationId) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let custody = removals.publication.as_mut().filter(|custody| custody.operation == operation)
            .ok_or(MountSourceError::Stale)?;
        custody.published = true;
        Ok(())
    }

pub(in crate::native_mount::lazy) fn finish_mount_custody(&self, operation: crate::OperationId, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        self.finish_detached_publication(operation, work)?;
        self.clear_mount_removals(operation, work)?;
        self.prune_detached_operation(Some(operation), work)?;
        let inputs = {
            let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            removals.publication = None;
            removals.selected_inputs.take()
        };
        drop(inputs);
        Ok(())
    }

pub(in crate::native_mount::lazy) fn finish_acknowledged_mount_custody<'a, 'work>(
        &'a self, operation: crate::OperationId, generation: crate::GenerationId,
        work: &'a mut MountSyncWork<'work>, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<crate::LazySnapshotRef>, MountSourceError>> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let original = original.clone();
        let cancellation = cancellation.clone();
        Box::pin(async move {
            work.items(1)?;
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            let Some(custody) = custody else {
                return self.lazy.selected_import_snapshot_for_generation(operation, generation, &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?).await.map(Some).map_err(native_lazy_error);
            };
            if custody.operation != operation { return Ok(None); }
            let Some(proof) = custody.selected else { return Ok(None); };
            if !custody.published {
                if !self.authored.shared_checkout().resolve_acknowledged_original_with_native_work(
                    operation, generation, &original, &cancellation, Some(self.authored.cancellation()),
                    self.authored.publication_guard(),
                ).await? {
                    return Ok(None);
                }
                self.mark_mount_published(operation)?;
            }
            let snapshot = self.lazy.selected_committed_snapshot(operation, generation, proof.checkpoint, proof.source, proof.source_config, proof.input, &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?;
            self.finish_mount_custody(operation, work)?;
            Ok(Some(snapshot))
        })
    }
}
