use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) fn removed_open_file(
        &self,
        id: FileId,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FileRecord, Arc<CheckoutDetachedFile<A, O>>), MountSourceError> {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = ViewGate::callback_owner();
                let record = self.authored.record_by_id(id, owner, carrier).await?;
                let file = self
                    .authored
                    .detached_file_from_record(record, metadata, owner, carrier)
                    .await?;
                Ok((record, file))
            })
        })
    }

    pub(super) fn reported(&self, file_id: FileId) -> bool {
        !self
            .linked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&file_id)
    }

    pub(super) fn detached_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        Ok(self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .get(&file_id)
            .filter(|entry| entry.ready)
            .map(|entry| Arc::clone(&entry.file)))
    }

    pub(super) fn has_open_source_handle(&self, file_id: FileId) -> Result<bool, MountSourceError> {
        let mut open = self
            .open_sources
            .lock()
            .map_err(|_| MountSourceError::Stale)?;
        let Some(handles) = open.get_mut(&file_id) else {
            return Ok(false);
        };
        handles.retain(|handle| handle.strong_count() != 0);
        let present = !handles.is_empty();
        if !present {
            open.remove(&file_id);
        }
        Ok(present)
    }

    pub(super) fn detached_for_path(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        if self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .is_empty()
        {
            return Ok(None);
        }
        if let Some(authored) = self.authored.lookup_with_meter(path, work)? {
            return self.detached_by_id(authored.node.file_id);
        }
        let (text, _allocation) = self.path(path, work)?;
        if self.is_removed(&text)? {
            return Err(MountSourceError::NotFound);
        }
        let file_id = self.runtime.wait(|| async {
            let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
            let lookup = self
                .lazy
                .lookup(&text, &mut accounting)
                .await
                .map_err(lazy_error)?;
            let id = self
                .lazy
                .stable_file_id_for_lookup(&text, &lookup.0, &mut accounting)
                .await;
            self.lazy
                .retire_lookup(lookup, &mut accounting)
                .map_err(lazy_error)?;
            id.map_err(lazy_error)
        })?;
        self.detached_by_id(file_id)
    }

    pub(super) fn finish_detached_publication(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        for (file_id, entry) in identities.iter_mut() {
            if !entry.ready {
                self.authored.record_projection_change(
                    &ViewChange::Node(*file_id),
                    &mut adapter::NativeDemandCarrier::Native(work),
                    || Ok(()),
                )?;
            }
            entry.ready = true;
            if let Some(epoch) = entry.pending_epoch.take() {
                entry.published_epoch = epoch;
            }
        }
        drop(identities);
        self.prune_detached()
    }

    pub(super) fn prune_detached(&self) -> Result<(), MountSourceError> {
        // After sync, retain identities held by source or shadow handles.
        let mut open = self
            .open_sources
            .lock()
            .map_err(|_| MountSourceError::Stale)?;
        open.retain(|_, handles| {
            handles.retain(|handle| handle.strong_count() != 0);
            !handles.is_empty()
        });
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        identities.retain(|id, entry| {
            open.contains_key(id)
                || !entry.ready
                || entry.pending_epoch.is_some()
                || Arc::strong_count(&entry.file) != 1
        });
        Ok(())
    }

    pub(super) fn abort_detached_publication(&self) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        identities.retain(|_, entry| entry.ready);
        for entry in identities.values_mut() {
            entry.pending_epoch = None;
        }
        Ok(())
    }

    pub(super) async fn dirty_detached_records(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::Owned<Vec<DirtyDetachedRecord>>, MountSourceError> {
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let detached = {
            let entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let mut files = crate::kernel::Owned::allocate(entries.len(), &mut accounting)
                .map_err(mount_engine_error)?;
            files.value.extend(
                entries
                    .values()
                    .filter(|entry| entry.ready && entry.pending_epoch.is_none())
                    .map(|entry| (Arc::clone(&entry.file), entry.published_epoch)),
            );
            files
        };
        let mut dirty = crate::kernel::Owned::allocate(detached.value.len(), &mut accounting)
            .map_err(mount_engine_error)?;
        for (file, published_epoch) in &detached.value {
            let (record, metadata, epoch) = file
                .snapshot(
                    &mut adapter::NativeDemandCarrier::Native(work),
                    work.cancellation(),
                )
                .await?;
            if epoch > *published_epoch {
                dirty.value.push((record, metadata, epoch));
            }
        }
        Ok(dirty)
    }

    pub(super) async fn stage_detached_records(
        &self,
        records: &[(FileRecord, FileMetadata)],
        dirty: &[DirtyDetachedRecord],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let owner = ViewGate::callback_owner();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        for (record, metadata) in records {
            let exists = self
                .detached
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .contains_key(&record.file_id);
            if !exists {
                let file = self
                    .authored
<<<<<<< HEAD
                    .detached_file_from_record(*record, *metadata, owner, &mut carrier)
                    .await?;
=======
                    .detached_file_from_record(
                        *record, *metadata, ViewGate::callback_owner(),
                        &mut adapter::NativeDemandCarrier::Native(work),
                    ).await?;
>>>>>>> goal/harness-final
                self.detached
                    .lock()
                    .map_err(|_| MountSourceError::Stale)?
                    .insert(
                        record.file_id,
                        DetachedIdentity {
                            file,
                            ready: false,
                            published_epoch: 0,
                            pending_epoch: None,
                        },
                    );
            }
        }
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        for (record, _, epoch) in dirty {
            let entry = identities
                .get_mut(&record.file_id)
                .ok_or(MountSourceError::Stale)?;
            entry.pending_epoch = Some(*epoch);
        }
        Ok(())
    }
}
