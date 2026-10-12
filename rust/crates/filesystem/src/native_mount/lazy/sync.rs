use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn recover_pending_mount_change(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let Some(committed) = self
            .lazy
            .mount_removal_publication(&mut crate::kernel::KernelAccounting::native(
                work,
                work.cancellation(),
            ))
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
            self.authored
                .sync_async_with_permit_carrier_guarded(
                    permit,
                    true,
                    &mut adapter::NativeDemandCarrier::Native(work),
                    work.cancellation(),
                    &mut self.authored.sdk_publication_policy(),
                )
                .await?;
        }
        let published = self
            .lazy
            .finish_mount_removals(&mut crate::kernel::KernelAccounting::native(
                work,
                work.cancellation(),
            ))
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
        let _mutation = self
            .source_view
            .write_stable(None)
            .await?
            .into_mutation_phase();
        self.sync_staged_with_permit(permit, &work).await
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError> {
        self.wait(|| self.sync_async())
    }

    pub(super) async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.recover_pending_mount_change(permit, work).await?;
        let (removals, captured_records) = self
            .removals
            .lock()
            .map_err(|_| MountSourceError::Stale)
            .map(|state| (state.paths.clone(), state.records.clone()))?;
        let paths = removals.keys().cloned().collect::<Vec<_>>();
        let dirty = self.dirty_detached_records(work).await?;
        if paths.is_empty() && dirty.is_empty() {
            self.authored
                .sync_async_with_permit_carrier_guarded(
                    permit,
                    false,
                    &mut adapter::NativeDemandCarrier::Native(work),
                    work.cancellation(),
                    &mut self.authored.sdk_publication_policy(),
                )
                .await?;
            return self.prune_detached();
        }
        let (operation_id, candidate, identity_records, epoch, stamp) = self
            .prepare_removed_identity_candidate(&removals, &dirty, work)
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
        if let Err(error) = self
            .stage_detached_records(&identity_records, &dirty, work)
            .await
        {
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
            .prepare_mount_removals(
                &paths,
                &[],
                &identity_records,
                idempotency_key,
                &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            )
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
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        use adapter::CaptureInputScratch;
        let mut scratch = adapter::NativeCaptureScratchAllocation::new(work)?;
        let mut namespaces = scratch.vec(paths.len())?;
        for path in &paths {
            let namespace = self
                .lazy
                .namespace_path(path, &mut accounting)
                .map_err(lazy_error)?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                namespace.allocation
            else {
                return Err(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ));
            };
            scratch.absorb(allocation);
            namespaces.push(namespace.value);
        }
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (installed, _allocation) = adapter::installed_changes(
            &candidate.base,
            &candidate.checkout,
            InstallScope::Paths(&namespaces),
            &mut carrier,
            work.cancellation(),
        )
        .await?;
        // The mutation lease excludes every other change to the checkout.
        if self
            .authored
            .shared_checkout()
            .finish_phase(
                candidate,
                epoch,
                stamp,
                &mut carrier,
                work.cancellation(),
                Some(self.authored.cancellation()),
                |carrier| {
                    adapter::record_view_change(
                        &self.authored.shared_checkout().ledger,
                        &ViewChange::Effect(&installed),
                        carrier,
                    )
                },
                Some(operation_id),
                |_, state| state.retained_operation_id(),
            )
            .await?
            .is_none()
        {
            return Err(MountSourceError::Stale);
        }
        self.authored
            .sync_async_with_permit_carrier_guarded(
                permit,
                true,
                &mut adapter::NativeDemandCarrier::Native(work),
                work.cancellation(),
                &mut self.authored.sdk_publication_policy(),
            )
            .await?;
        if !self
            .lazy
            .finish_mount_removals(&mut crate::kernel::KernelAccounting::native(
                work,
                work.cancellation(),
            ))
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
        work: &crate::NativeMaterializationWork,
    ) -> Result<
        (
            crate::OperationId,
            CheckoutCandidate<A, O>,
            Vec<(FileRecord, FileMetadata)>,
            u64,
            ViewStamp,
        ),
        MountSourceError,
    > {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (mut transaction, epoch, stamp) = self
            .authored
            .shared_checkout()
            .prepare_native_phase_with_carrier(
                &mut carrier,
                work.cancellation(),
                Some(self.authored.cancellation()),
            )
            .await?;
        let candidate = &mut transaction.checkout;
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let mut identity_records = BTreeMap::new();
        for (path, removed_id) in removals {
            let namespace = self
                .lazy
                .namespace_path(path, &mut accounting)
                .map_err(lazy_error)?;
            let lookup = candidate
                .lookup_no_follow_with_metadata(&namespace.value, work, work.cancellation())
                .await
                .map_err(mount_engine_error)?;
            let Some(lookup) = lookup else {
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
                    namespace.value,
                    Some(*removed_id),
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(mount_engine_error)?;
        }
        // Update surviving hard links from the detached identity, without rereading the source.
        let mut replacements = Vec::new();
        for (file_id, record, _, _) in dirty {
            let live = match candidate
                .read_file_record_by_id(*file_id, work, work.cancellation())
                .await
            {
                Ok(record) => Some(record),
                Err(crate::FsError::NotFound) => None,
                Err(error) => return Err(mount_engine_error(error)),
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
                    .mutate(chunk.to_vec(), work, work.cancellation())
                    .await
                    .map_err(mount_engine_error)?;
            }
        }
        let identity_records = identity_records.into_values().collect();
        let operation_id = crate::OperationId::new();
        Ok((operation_id, transaction, identity_records, epoch, stamp))
    }
}
