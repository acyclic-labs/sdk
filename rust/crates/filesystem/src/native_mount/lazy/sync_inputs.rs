use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn clear_mount_removals(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(removals.paths.len())?;
        let removing = removals.paths.values().filter(|removed| removed.operation == Some(operation)).count();
        let cleanup = crate::WorkCounters {
            items_examined: (removals.paths.len() as u64).checked_add(removing as u64)
                .and_then(|items| items.checked_add(removals.records.len() as u64))
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
            ..crate::WorkCounters::default()
        };
        work.check().map_err(mount_demand_error)?;
        work.admit(&cleanup)?;
        let MountedRemovals { paths, records, .. } = &mut *removals;
        paths.retain(|_, removed| {
            if removed.operation != Some(operation) { return true; }
            if let Some(record) = records.get_mut(&removed.file_id) {
                record.paths = record.paths.checked_sub(1).expect("captured identity has a removal owner");
            }
            false
        });
        records.retain(|_, record| record.paths != 0);
        work.record(cleanup).map_err(mount_demand_error)?;
        Ok(())
    }

pub(in crate::native_mount::lazy) async fn snapshot_sync_inputs(
        &self, work: &mut MountSyncWork<'_>, cancellation: crate::CancellationToken,
    ) -> Result<MountSyncInputs, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        drop(work.lock_checkout(&self.authored, cancellation.clone()).await?);
        let (paths, removed_ids) = {
            let state = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let mut paths = work.owned_vec::<String>(state.paths.len())?;
            let mut ids = work.owned_vec::<FileId>(state.paths.len())?;
            for (path, removed) in &state.paths {
                work.items(1)?;
                paths.push_cloned_text(&path.text, work)?;
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<FileId>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(&copied)?;
                ids.value.push(removed.file_id);
                work.record(copied).map_err(mount_demand_error)?;
            }
            (paths, ids)
        };
        let detached = {
            let state = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let mut files = work.owned_vec::<(FileId, Arc<CheckoutDetachedFile<A, O>>, u64)>(state.len())?;
            for (id, entry) in state.iter() {
                work.items(1)?;
                if entry.ready {
                    let copied = crate::WorkCounters {
                        bytes_copied: std::mem::size_of::<(FileId, Arc<CheckoutDetachedFile<A, O>>, u64)>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    work.admit(&copied)?;
                    files.value.push((*id, Arc::clone(&entry.file), entry.published_epoch));
                    work.record(copied).map_err(mount_demand_error)?;
                }
            }
            files
        };
        let mut dirty = work.owned_vec::<DirtyDetachedRecord>(0)?;
        for (id, file, published) in &detached.value {
            work.items(1)?;
            let (record, metadata, epoch) = file.snapshot(&mut work.meter, &cancellation).await?;
            if epoch > *published {
                if dirty.value.capacity() == 0 { dirty = work.owned_vec(detached.value.len())?; }
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<DirtyDetachedRecord>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(&copied)?;
                dirty.value.push((*id, record, metadata, epoch));
                work.record(copied).map_err(mount_demand_error)?;
            }
        }
        drop(detached);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(MountSyncInputs { paths, removed_ids, dirty })
    }

pub(in crate::native_mount::lazy) async fn prepare_sync_candidate_from_inputs(
        &self, inputs: MountSyncInputs, work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<PreparedMountSync<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let (candidate, epoch, stamp, identity_records) = self.prepare_removed_identity_candidate_measured(
            &inputs.paths.value, &inputs.removed_ids.value, &inputs.dirty.value, work, cancellation,
        ).await?;
        let records = {
            let captured = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let captured_records = captured.records.values().map(|entry| (&entry.value.0, &entry.value.1));
            let candidate_records = identity_records.value.iter().map(|(record, metadata)| (record, metadata));
            let dirty_records = inputs.dirty.value.iter().map(|(_, record, metadata, _)| (record, metadata));
            let count = mount_record_union(
                captured_records.clone(), candidate_records.clone(), dirty_records.clone(), None, &mut work.meter,
            )?;
            let mut records = work.owned_vec(count)?;
            mount_record_union(
                captured_records, candidate_records, dirty_records, Some(&mut records.value), &mut work.meter,
            )?;
            records
        };
        drop(identity_records);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(PreparedMountSync { candidate, epoch, stamp, inputs, records })
    }

#[cfg(test)]
    pub(in crate::native_mount::lazy) async fn prepare_removed_identity_candidate(
        &self,
        removals: &BTreeMap<String, FileId>,
        dirty: &[DirtyDetachedRecord],
    ) -> Result<(crate::OperationId, CheckoutCandidate<A, O>, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let paths = removals.keys().cloned().collect::<Vec<_>>();
        let ids = removals.values().copied().collect::<Vec<_>>();
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        let (candidate, _, _, records) = self.prepare_removed_identity_candidate_measured(
            &paths, &ids, dirty, &mut work, self.authored.cancellation(),
        ).await?;
        let operation_id = self.authored.shared_checkout().lock().await.retained_operation_id();
        Ok((operation_id, candidate, records))
    }

pub(in crate::native_mount::lazy) async fn prepare_removed_identity_candidate_measured(
        &self,
        paths: &[String],
        removed_ids: &[FileId],
        dirty: &[DirtyDetachedRecord],
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let selected = self.removals.lock().map_err(|_| MountSourceError::Stale)?
            .publication.filter(|custody| custody.selected.is_some() && !custody.published);
        let (mut transaction, epoch, stamp) = self.authored.shared_checkout()
            .prepare_native_phase_for_operation_with_carrier(
                selected.map(|custody| custody.operation), &mut work.meter,
                cancellation, Some(self.authored.cancellation()),
            ).await?;
        let candidate = &mut transaction.checkout;
        let mut records = work.owned_vec::<(FileRecord, FileMetadata)>(paths.len())?;
        for (path, removed_id) in paths.iter().zip(removed_ids) {
            work.items(1)?;
            let mut namespace = work.namespace_buffer(path, candidate.volume_config())?;
            let lookup = if let Some(original) = work.meter.original_native_work() {
                candidate.lookup_no_follow_with_metadata(
                    namespace.value.as_ref().expect("original removal namespace"), &original, cancellation,
                ).await.map_err(mount_engine_error)?
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.lookup_no_follow_with_metadata(
                    namespace.value.as_ref().expect("original removal namespace"), budget, cancellation,
                ).await;
                work.accept_sdk(result, prior_live, mount_engine_error)?
            };
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            let Some(lookup) = lookup else {
                drop(namespace);
                continue;
            };
            if lookup.record.file_id != *removed_id {
                drop(namespace);
                continue;
            }
            if lookup.record.kind != FileKind::Regular { return Err(MountSourceError::Stale); }
            mount_upsert_record(&mut records.value, (lookup.record, lookup.metadata), &mut work.meter)?;
            if let Some(original) = work.meter.original_native_work() {
                let mut operations = work.owned_vec(1)?;
                operations.absorb_buffer_guard(&mut namespace)?;
                operations.value.push(Mutation::Remove {
                    path: namespace.value.take().expect("original removal namespace"),
                    expected_file_id: MetadataField::Value(*removed_id),
                });
                let (operations, allocation) = operations.take_native_parts()?;
                candidate.mutate_owned_with_accounting(crate::kernel::Owned { value: operations, bytes: allocation.bytes(), allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) }, &mut crate::kernel::allocation::KernelAccounting::native(&original, cancellation), cancellation)
                    .await.map_err(mount_engine_error)?;
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.mutate(vec![crate::kernel::Mutation::Remove { path: namespace.value.take().expect("original removal namespace"), expected_file_id: (Some(*removed_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], budget, cancellation).await;
                work.accept_sdk(result, prior_live, mount_engine_error)?;
            }
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            drop(namespace);
        }
        for index in 0..dirty.len() {
            let (file_id, record, _, _) = dirty[index];
            work.items(1)?;
            let result = if let Some(original) = work.meter.original_native_work() {
                candidate.read_file_record_by_id(file_id, &original, cancellation)
                    .await.map_err(mount_engine_error)
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.read_file_record_by_id(file_id, budget, cancellation).await;
                work.accept_sdk(result, prior_live, mount_engine_error)
            };
            let live = match result {
                Ok(record) => Some(record),
                Err(error) if matches!(error.semantic_error(), MountSourceError::NotFound) => None,
                Err(error) => return Err(error),
            };
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            if let Some(live) = live {
                if live.kind != record.kind { return Err(MountSourceError::Stale); }
                let mut replacements = work.owned_vec(1)?;
                replacements.value.push(Mutation::File {
                    file_id: file_id, mutation: FileMutation::ReplaceRecord { record: record },
                });
                if let Some(original) = work.meter.original_native_work() {
                    let (operations, allocation) = replacements.take_native_parts()?;
                    candidate.mutate_owned_with_accounting(crate::kernel::Owned { value: operations, bytes: allocation.bytes(), allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) }, &mut crate::kernel::allocation::KernelAccounting::native(&original, cancellation), cancellation)
                        .await.map_err(mount_engine_error)?;
                } else {
                    let (budget, prior_live) = work.sdk_scope()?;
                    let result = candidate.mutate(
                        std::mem::take(&mut replacements.value), budget, cancellation,
                    ).await;
                    work.accept_sdk(result, prior_live, mount_engine_error)?;
                }
                work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
                drop(replacements);
            }
        }
        Ok((transaction, epoch, stamp, records))
    }

/// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _writer = self.source_view.write().await;
        if self.lazy.binding().await.is_ok() {
            let result = self.authored.advance_to_head_async().await;
            self.cursors.clear();
            return result;
        }
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy.rebind_source(&crate::lazy_workspace::LazyTreapWork::new(
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )).await.map_err(lazy_error)?;
            // Every unauthored node may read differently from the new epoch.
            self.authored
                .record_projection_change(&ViewChange::Everything);
            self.cursors.clear();
            Ok(())
        }
        .await;

        if result.is_ok() {
            self.source_view.finish_transition();
        }
        result
    }
}
