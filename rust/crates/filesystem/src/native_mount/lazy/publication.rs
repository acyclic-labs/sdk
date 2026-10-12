use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(super) fn finish_detached_publication(&self) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        for (file_id, entry) in identities.iter_mut() {
            if !entry.ready {
                self.authored
                    .record_projection_change(&ViewChange::Node(*file_id));
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
        // Called only after a successful sync under the stable source-view gate.
        // A source handle re-fetches this identity on every operation, while a
        // shadow open holds the detached file directly. Keep either kind live.
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

pub(super) fn dirty_detached_records(&self) -> Result<Vec<DirtyDetachedRecord>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let detached = self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .iter()
            .filter(|(_, entry)| entry.ready && entry.pending_epoch.is_none())
            .map(|(id, entry)| (*id, Arc::clone(&entry.file), entry.published_epoch))
            .collect::<Vec<_>>();
        let mut dirty = Vec::new();
        for (id, file, published_epoch) in detached {
            let (record, metadata, epoch) = file.snapshot()?;
            if epoch > published_epoch {
                dirty.push((id, record, metadata, epoch));
            }
        }
        Ok(dirty)
    }

pub(super) fn stage_detached_records(
        &self,
        records: &[(FileRecord, FileMetadata)],
        dirty: &[DirtyDetachedRecord],
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        for (record, metadata) in records {
            let exists = self
                .detached
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .contains_key(&record.file_id);
            if !exists {
                let file = self
                    .authored
                    .detached_file_from_record(*record, *metadata)?;
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
        for (id, _, _, epoch) in dirty {
            let entry = identities.get_mut(id).ok_or(MountSourceError::Stale)?;
            entry.pending_epoch = Some(*epoch);
        }
        Ok(())
    }

pub(super) async fn recover_pending_mount_change(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let Some(committed) = self
            .lazy
            .mount_removal_publication()
            .await
            .map_err(lazy_error)?
        else {
            return Ok(());
        };
        // An ambiguous acknowledgement retains the exact checkout candidate.
        let retained = self
            .authored
            .shared_checkout()
            .lock()
            .await
            .has_retained_operation();
        if retained {
            self.authored.sync_async_with_permit_force(permit).await?;
        }
        let published = self
            .lazy
            .finish_mount_removals()
            .await
            .map_err(lazy_error)?;
        if committed || retained {
            if !published {
                return Err(MountSourceError::Stale);
            }
            self.clear_mount_removals()?;
            self.finish_detached_publication()?;
        } else {
            self.abort_detached_publication()?;
        }
        Ok(())
    }

/// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?;
        self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

/// Publishes pending authored writes under one authority lease permit.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?;
        self.sync_staged_with_permit(permit).await
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

pub(super) async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.recover_pending_mount_change(permit).await?;
        let (removals, captured_records) = self
            .removals
            .lock()
            .map_err(|_| MountSourceError::Stale)
            .map(|state| (state.paths.clone(), state.records.clone()))?;
        let paths = removals.keys().cloned().collect::<Vec<_>>();
        let dirty = self.dirty_detached_records()?;
        if paths.is_empty() && dirty.is_empty() {
            self.authored.sync_async_with_permit(permit).await?;
            return self.prune_detached();
        }
        let (operation_id, candidate, identity_records) = self
            .prepare_removed_identity_candidate(&removals, &dirty)
            .await?;
        let mut records = captured_records;
        records.extend(
            identity_records
                .into_iter()
                .map(|(record, metadata)| (record.file_id, (record, metadata))),
        );
        records.extend(
            dirty
                .iter()
                .map(|(_, record, metadata, _)| (record.file_id, (*record, *metadata))),
        );
        let identity_records = records.into_values().collect::<Vec<_>>();
        if let Err(error) = self.stage_detached_records(&identity_records, &dirty) {
            self.authored
                .shared_checkout()
                .lock()
                .await
                .clear_retained_operation(operation_id);
            self.abort_detached_publication()?;
            return Err(error);
        }
        let idempotency_key = IdempotencyKey::from_bytes(operation_id.into_bytes());
        if let Err(error) = self
            .lazy
            .prepare_mount_removals(&paths, &identity_records, idempotency_key)
            .await
        {
            self.authored
                .shared_checkout()
                .lock()
                .await
                .clear_retained_operation(operation_id);
            self.abort_detached_publication()?;
            return Err(lazy_error(error));
        }
        let removed = paths
            .iter()
            .map(|path| self.lazy.namespace_path(path).map_err(lazy_error))
            .collect::<Result<Vec<_>, _>>()?;
        let installed = candidate
            .installed_changes(InstallScope::Paths(&removed), self.authored.cancellation())
            .await;
        // The mutation lease excludes every other change to the checkout.
        if !self
            .authored
            .shared_checkout()
            .lock()
            .await
            .install_candidate(candidate, &installed)
        {
            return Err(MountSourceError::Stale);
        }
        self.authored.sync_async_with_permit_force(permit).await?;
        if !self
            .lazy
            .finish_mount_removals()
            .await
            .map_err(lazy_error)?
        {
            return Err(MountSourceError::Stale);
        }
        self.clear_mount_removals()?;
        self.finish_detached_publication()?;
        Ok(())
    }

pub(super) fn clear_mount_removals(&self) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        removals.paths.clear();
        removals.records.clear();
        Ok(())
    }

pub(super) async fn prepare_removed_identity_candidate(
        &self,
        removals: &BTreeMap<String, FileId>,
        dirty: &[DirtyDetachedRecord],
    ) -> Result<
        (
            crate::OperationId,
            CheckoutCandidate<A, O>,
            Vec<(FileRecord, FileMetadata)>,
        ),
        MountSourceError,
    >
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.authored.shared_checkout().lock().await;
        let mut transaction = checkout.candidate()?;
        let candidate = &mut transaction.checkout;
        let mut identity_records = BTreeMap::new();
        for (path, removed_id) in removals {
            let namespace = self.lazy.namespace_path(path).map_err(lazy_error)?;
            let lookup = candidate
                .lookup_no_follow_with_metadata(
                    &namespace,
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
                .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
            let Some(lookup) = lookup.value else {
                continue;
            };
            if lookup.record.file_id != *removed_id {
                continue;
            }
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Stale);
            }
            identity_records
                .entry(*removed_id)
                .or_insert((lookup.record, lookup.metadata));
            candidate
                .remove(
                    namespace,
                    Some(*removed_id),
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
                .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
        }
        // A detached handle can still write through a removed name while a
        // sibling hard link is already authored. Refresh that live identity
        // directly; resolving it by a surviving path would require a scan and
        // could incorrectly re-read stale source bytes.
        let mut replacements = Vec::new();
        for (file_id, record, _, _) in dirty {
            let live = match candidate
                .read_file_record_by_id(
                    *file_id,
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
            {
                Ok(receipt) => Some(receipt.value),
                Err(failure) if matches!(failure.error, crate::FsError::NotFound) => None,
                Err(failure) => {
                    return Err(MountSourceError::Engine(failure.error.to_string()));
                }
            };
            if let Some(live) = live {
                if live.kind != record.kind {
                    return Err(MountSourceError::Stale);
                }
                replacements.push(Mutation::File {
                    file_id: *file_id,
                    mutation: FileMutation::ReplaceRecord { record: *record },
                });
            }
        }
        if !replacements.is_empty() {
            let maximum =
                usize::try_from(candidate.volume_config().limits.maximum_mutations_per_batch)
                    .unwrap_or(usize::MAX)
                    .max(1);
            for chunk in replacements.chunks(maximum) {
                candidate
                    .mutate(
                        chunk.to_vec(),
                        crate::WorkBudget::UNBOUNDED,
                        self.authored.cancellation(),
                    )
                    .await
                    .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
            }
        }
        let identity_records = identity_records.into_values().collect();
        let operation_id = checkout.retained_operation_id();
        Ok((operation_id, transaction, identity_records))
    }

/// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _writer = self.source_view.write().await;
        // A binding to its source's current epoch reads the same after a
        // rebind: the authored advance then records exactly what it changed,
        // and nothing at all when head did not move.
        if self.lazy.binding().await.is_ok() {
            let result = self.authored.advance_to_head_async().await;
            self.cursors.clear();
            return result;
        }
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy.rebind_source().await.map_err(lazy_error)?;
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
