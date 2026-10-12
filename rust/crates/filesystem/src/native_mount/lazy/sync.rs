use super::*;
use std::future::Future;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn publish<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        before_publication: &mut F,
        selected: Option<(
            crate::LazySnapshotRef,
            &crate::SelectedImportPlan,
            crate::OperationId,
        )>,
    ) -> Result<crate::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        use crate::lazy_workspace::NativeSnapshotContext;
        use adapter::CaptureInputScratch;
        let mut accounting_failure = None;
        let result: Result<crate::NativePublicationSnapshot, E> = async {
            let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
            let mut carrier = adapter::NativeDemandCarrier::Native(work);
            let selected_proof = selected
                .map(|(destination, plan, _)| {
                    plan.input_digest(destination, &mut accounting)
                        .map(|input| (destination, plan.source, plan.source_config, input))
                        .map_err(lazy_error)
                })
                .transpose()?;
            let remembered = self
                .removals
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .publication;
            if let (Some((operation, proof)), Some((_, _, requested))) = (remembered, selected) {
                if operation != requested || proof != selected_proof {
                    return Err(MountSourceError::Stale.into());
                }
            }
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
            let operation = remembered.map(|(operation, _)| operation);
            let committed = match operation.or(selected.map(|(_, _, operation)| operation)) {
                Some(operation) => self
                    .lazy
                    .operation_generation(
                        IdempotencyKey::from_bytes(operation.into_bytes()),
                        &mut accounting,
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
                let context = if let Some((destination, source, source_config, input)) = proof {
                    let (snapshot, original_input) = self
                        .lazy
                        .selected_snapshot(operation, generation, &mut accounting)
                        .await
                        .map_err(lazy_error)?;
                    if original_input != input {
                        return Err(MountSourceError::Stale.into());
                    }
                    let cut = self
                        .lazy
                        .publication_cut(Some(operation), &mut accounting)
                        .await
                        .map_err(lazy_error)?;
                    if cut.workspace_id != snapshot.workspace_id
                        || cut.source != snapshot.source
                        || cut.overlay != snapshot.overlay
                        || cut.shadows != snapshot.shadows
                    {
                        return Err(MountSourceError::Stale.into());
                    }
                    let context = NativeSnapshotContext::Selected { cut, input };
                    *acknowledged = Some(crate::NativePublicationSnapshot::published(
                        context, operation, generation,
                    ));
                    if retained == Some(operation)
                        && !self
                            .authored
                            .shared_checkout()
                            .resolve_acknowledged_original_with_native_work(
                                operation,
                                generation,
                                work,
                                cancellation,
                                Some(self.authored.cancellation()),
                                self.authored.publication_guard(),
                            )
                            .await?
                    {
                        return Err(MountSourceError::Indeterminate.into());
                    }
                    self.lazy
                        .finish_selected_import(
                            operation,
                            generation,
                            destination,
                            source,
                            source_config,
                            input,
                            &mut accounting,
                        )
                        .await
                        .map_err(lazy_error)?;
                    context
                } else {
                    let context = NativeSnapshotContext::Lazy(
                        self.lazy
                            .publication_cut(Some(operation), &mut accounting)
                            .await
                            .map_err(lazy_error)?,
                    );
                    *acknowledged = Some(crate::NativePublicationSnapshot::published(
                        context, operation, generation,
                    ));
                    if retained == Some(operation)
                        && !self
                            .authored
                            .shared_checkout()
                            .resolve_acknowledged_original_with_native_work(
                                operation,
                                generation,
                                work,
                                cancellation,
                                Some(self.authored.cancellation()),
                                self.authored.publication_guard(),
                            )
                            .await?
                    {
                        return Err(MountSourceError::Indeterminate.into());
                    }
                    if !self
                        .lazy
                        .finish_mount_removals(&mut accounting)
                        .await
                        .map_err(lazy_error)?
                    {
                        return Err(MountSourceError::Stale.into());
                    }
                    context
                };
                if remembered.is_some_and(|(id, _)| id == operation) {
                    self.finish_detached_publication()?;
                    self.clear_mount_removals()?;
                }
                return Ok(crate::NativePublicationSnapshot::published(
                    context, operation, generation,
                ));
            }
            if let Some(operation) = operation {
                if retained == Some(operation) {
                    let context = self
                        .authored
                        .retained_publication_context_with_carrier(&mut carrier, cancellation)
                        .await?;
                    let context = match context {
                        Some(context) => context,
                        None => {
                            let cut = self
                                .lazy
                                .publication_cut(Some(operation), &mut accounting)
                                .await
                                .map_err(lazy_error)?;
                            match remembered.and_then(|(_, proof)| proof) {
                                Some((_, _, _, input)) => {
                                    NativeSnapshotContext::Selected { cut, input }
                                }
                                None => NativeSnapshotContext::Lazy(cut),
                            }
                        }
                    };
                    let published = self
                        .authored
                        .sync_async_with_permit_carrier_guarded_context_with_ack(
                            permit,
                            true,
                            &mut carrier,
                            cancellation,
                            before_publication,
                            Some(context),
                            acknowledged,
                        )
                        .await
                        .map_err(|(error, accounting)| {
                            accounting_failure = accounting;
                            error
                        })?;
                    if let Some((destination, source, config, input)) =
                        remembered.and_then(|(_, proof)| proof)
                    {
                        self.lazy
                            .finish_selected_import(
                                operation,
                                published.generation(),
                                destination,
                                source,
                                config,
                                input,
                                &mut accounting,
                            )
                            .await
                            .map_err(lazy_error)?;
                    } else if !self
                        .lazy
                        .finish_mount_removals(&mut accounting)
                        .await
                        .map_err(lazy_error)?
                    {
                        return Err(MountSourceError::Stale.into());
                    }
                    self.finish_detached_publication()?;
                    self.clear_mount_removals()?;
                    return Ok(published);
                }
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
            let mut input = adapter::NativeCaptureScratchAllocation::new(work)?;
            let (removals, captured_records) = {
                let state = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                let mut paths = input.vec(state.paths.len())?;
                for (path, (id, _)) in &state.paths {
                    input.items(1)?;
                    let copied = crate::WorkCounters {
                        bytes_copied: path.len() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    input.admit(copied)?;
                    let mut bytes = input.vec(path.len())?;
                    bytes.extend_from_slice(path.as_bytes());
                    input.record(copied)?;
                    paths.push((
                        String::from_utf8(bytes).expect("copied String is UTF-8"),
                        *id,
                    ));
                }
                let records = state.records.clone();
                (paths, records)
            };
            let dirty = self.dirty_detached_records(work).await?;
            if removals.is_empty() && dirty.is_empty() && selected.is_none() {
                let context = NativeSnapshotContext::Lazy(
                    self.lazy
                        .publication_cut(None, &mut accounting)
                        .await
                        .map_err(lazy_error)?,
                );
                let published = self
                    .authored
                    .sync_async_with_permit_carrier_guarded_context_with_ack(
                        permit,
                        false,
                        &mut carrier,
                        cancellation,
                        before_publication,
                        Some(context),
                        acknowledged,
                    )
                    .await
                    .map_err(|(error, accounting)| {
                        accounting_failure = accounting;
                        error
                    })?;
                self.prune_detached()?;
                return Ok(published);
            }
            let (fresh_operation, mut candidate, identities, epoch, stamp) = self
                .prepare_removed_identity_candidate(&removals, &dirty, work)
                .await?;
            let mut paths = input.vec(removals.len())?;
            paths.extend(removals.into_iter().map(|(path, _)| path));
            let operation = selected.map_or(fresh_operation, |(_, _, operation)| operation);
            let effects = match selected {
                Some((destination, plan, _)) => Some(
                    self.lazy
                        .prepare_selected_import_into_checkout(
                            destination,
                            plan,
                            &mut candidate.checkout,
                            operation,
                            &mut accounting,
                        )
                        .await
                        .map_err(lazy_error)?,
                ),
                None => None,
            };
            let mut records = captured_records;
            records.extend(
                identities
                    .into_iter()
                    .map(|(record, metadata)| (record.file_id, (record, metadata))),
            );
            if let Some(effects) = &effects {
                records.extend(
                    effects
                        .records
                        .iter()
                        .map(|(record, metadata)| (record.file_id, (*record, *metadata))),
                );
            }
            records.extend(
                dirty
                    .iter()
                    .map(|(_, record, metadata, _)| (record.file_id, (*record, *metadata))),
            );
            let records = records.into_values().collect::<Vec<_>>();
            if let Err(error) = self.stage_detached_records(&records, &dirty, work).await {
                self.abort_detached_publication()?;
                return Err(error.into());
            }
            self.removals
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .publication = Some((operation, selected_proof));
            self.lazy
                .prepare_mount_removals(
                    &paths,
                    effects
                        .as_ref()
                        .map_or(&[], |effects| effects.paths.as_slice()),
                    &records,
                    IdempotencyKey::from_bytes(operation.into_bytes()),
                    &mut accounting,
                )
                .await
                .map_err(lazy_error)?;
            let cut = self
                .lazy
                .publication_cut(Some(operation), &mut accounting)
                .await
                .map_err(lazy_error)?;
            let context = match selected_proof {
                Some((_, _, _, input)) => NativeSnapshotContext::Selected { cut, input },
                None => NativeSnapshotContext::Lazy(cut),
            };
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
                    )
                    .into());
                };
                scratch.absorb(allocation);
                namespaces.push(namespace.value);
            }
            let scope = match &effects {
                Some(effects) => InstallScope::CombinedPaths(&namespaces, &effects.changes),
                None => InstallScope::Paths(&namespaces),
            };
            let (installed, _allocation) = adapter::installed_changes(
                &candidate.base,
                &candidate.checkout,
                scope,
                &mut carrier,
                cancellation,
            )
            .await?;
            if self
                .authored
                .shared_checkout()
                .finish_phase(
                    candidate,
                    epoch,
                    stamp,
                    &mut carrier,
                    cancellation,
                    Some(self.authored.cancellation()),
                    |carrier| {
                        adapter::record_view_change(
                            &self.authored.shared_checkout().ledger,
                            &ViewChange::Effect(&installed),
                            carrier,
                        )
                    },
                    Some(operation),
                    |_, state| state.retained_operation_id(),
                )
                .await?
                .is_none()
            {
                return Err(MountSourceError::Stale.into());
            }
            let published = self
                .authored
                .sync_async_with_permit_carrier_guarded_context_with_ack(
                    permit,
                    true,
                    &mut carrier,
                    cancellation,
                    before_publication,
                    Some(context),
                    acknowledged,
                )
                .await
                .map_err(|(error, accounting)| {
                    accounting_failure = accounting;
                    error
                })?;
            if let Some((destination, source, config, input)) = selected_proof {
                self.lazy
                    .finish_selected_import(
                        operation,
                        published.generation(),
                        destination,
                        source,
                        config,
                        input,
                        &mut accounting,
                    )
                    .await
                    .map_err(lazy_error)?;
            } else if !self
                .lazy
                .finish_mount_removals(&mut accounting)
                .await
                .map_err(lazy_error)?
            {
                return Err(MountSourceError::Stale.into());
            }
            self.finish_detached_publication()?;
            self.clear_mount_removals()?;
            Ok(published)
        }
        .await;
        if result.is_err() {
            let cleanup: Result<(), MountSourceError> = async {
                let mut carrier = adapter::NativeDemandCarrier::Native(work);
                let retained = self
                    .authored
                    .shared_checkout()
                    .lock_with_carrier(
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
            .await;
            if let Err(error) = cleanup {
                accounting_failure = accounting_failure.or(Some(error));
            }
        }
        let accounting = work
            .admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork);
        match (result, accounting) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Ok(_), Err(error)) => Err((E::from(error), accounting_failure)),
            (Err(error), Ok(())) => Err((error, accounting_failure)),
            (Err(error), Err(accounting)) => Err((error, accounting_failure.or(Some(accounting)))),
        }
    }

    pub(super) fn clear_mount_removals(&self) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        removals.paths.clear();
        removals.records.clear();
        removals.publication = None;
        Ok(())
    }

    pub(super) async fn prepare_removed_identity_candidate(
        &self,
        removals: &[(String, FileId)],
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
            let mut namespace = self
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
            let mut operations =
                crate::kernel::Owned::allocate(1, &mut accounting).map_err(mount_engine_error)?;
            operations.bytes = operations
                .bytes
                .checked_add(namespace.bytes)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            operations
                .allocation
                .absorb(&mut namespace.allocation)
                .map_err(mount_engine_error)?;
            operations.value.push(Mutation::Remove {
                path: namespace.value,
                expected_file_id: MetadataField::Value(*removed_id),
            });
            candidate
                .mutate_owned_with_accounting(operations, &mut accounting, work.cancellation())
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
                let mut operations = crate::kernel::Owned::allocate(chunk.len(), &mut accounting)
                    .map_err(mount_engine_error)?;
                operations.value.extend(chunk.iter().cloned());
                candidate
                    .mutate_owned_with_accounting(operations, &mut accounting, work.cancellation())
                    .await
                    .map_err(mount_engine_error)?;
            }
        }
        let identity_records = identity_records.into_values().collect();
        let operation_id = crate::OperationId::new();
        Ok((operation_id, transaction, identity_records, epoch, stamp))
    }
}
