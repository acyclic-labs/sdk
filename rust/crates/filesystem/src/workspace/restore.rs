use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Restores an exact generation under the supplied publication permit.
    pub async fn restore_generation(
        &self,
        generation: &Generation<A, O>,
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<WorkspaceRestore<Generation<A, O>>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let outcome = self
            .volume
            .fs
            .restore_workspace_generation(
                &self.volume,
                generation.id,
                if_current,
                idempotency_key.operation_id(),
                permit,
            )
            .await?;
        let restored = |id| Generation { workspace: self.clone(), id, publication_closure: None };
        Ok(match outcome {
            WorkspaceRestore::Restored(id) => WorkspaceRestore::Restored(restored(id)),
            WorkspaceRestore::AlreadyRestored(id) => {
                WorkspaceRestore::AlreadyRestored(restored(id))
            }
            WorkspaceRestore::Current(id) => WorkspaceRestore::Current(restored(id)),
            WorkspaceRestore::Stale(id) => WorkspaceRestore::Stale(restored(id)),
            WorkspaceRestore::Fenced => WorkspaceRestore::Fenced,
            WorkspaceRestore::IdempotencyConflict => WorkspaceRestore::IdempotencyConflict,
        })
    }

    /// Restores selected records through the original publication engine with
    pub async fn restore_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            false, before_publication,
        ).await
    }

    /// Restores a caller-expanded selected subtree record union with one seal.
    pub async fn restore_selected_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            true, before_publication,
        ).await
    }

    /// Restores the caller-expanded frozen selection at the original native seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn restore_selected_paths_from_with_permit_guarded_with_native_work<'original, E, F, Fut>(
        &self, source: &Generation<A, O>, paths: &[String], if_current: GenerationId,
        idempotency_key: IdempotencyKey, permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(E::from(WorkspaceError::ForeignGeneration));
        }
        let current = self.head(original, cancellation).await.map_err(E::from)?;
        if current.id != if_current {
            return Ok(TransactionCommit::Conflict { actual: current });
        }
        let mut transaction = self.begin_pinned_transaction(
            &current, idempotency_key, original, cancellation,
        ).await.map_err(E::from)?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        transaction.checkout.validate_native_work(original).map_err(FsError::from)?;
        let source_root = self.restore_source_root_with_accounting(source, &mut accounting, cancellation)
            .await.map_err(E::from)?;
        transaction.restore_paths_from_root_with_accounting(
            source, paths, true, source_root, &mut accounting, cancellation,
        ).await.map_err(E::from)?;
        transaction.commit_with_native_work(
            None, permit, original, cancellation, before_publication,
        ).await
    }

    async fn restore_path_records<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        selected_subtrees: bool,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(move |budget| async move {
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let current = merge_workspace_backend(
                    self.head(budget, cancellation).await, &mut work, 0,
                )?.value;
                if current.id != if_current {
                    return Ok(TransactionCommit::Conflict { actual: current });
                }
                let source_root = {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    self.restore_source_root_with_accounting(source, &mut accounting, cancellation).await?
                };
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let mut transaction = merge_workspace_backend(
                    self.begin_pinned_transaction(
                        &current, idempotency_key, allowance, cancellation,
                    ).await, &mut work, allocations.live_bytes(),
                )?.value;
                allocations.claim_bytes(transaction.checkout.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    transaction.restore_paths_from_root_with_accounting(
                        source, paths, selected_subtrees, source_root, &mut accounting, cancellation,
                    ).await?;
                }
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let committed = merge_workspace_backend(
                    transaction.commit(None, permit, allowance, cancellation, |budget| {
                            let future = before_publication(budget);
                            async move { future.await.map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, WorkspaceError::from)) }
                        }).await, &mut work, allocations.live_bytes(),
                )?;
                Ok(committed.value)
            }.await;
            work.verify(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            result.map(|value| crate::OperationReceipt { value, work })
                .map_err(|error| crate::OperationFailure::new(error, work))
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }

    async fn restore_source_root_with_accounting(
        &self, source: &Generation<A, O>, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        source.workspace.open_checkout(
            GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    async fn restore_paths_from_root_with_accounting(
        &mut self,
        source: &Generation<A, O>,
        paths: &[String],
        selected_subtrees: bool,
        mut source_root: crate::kernel::Owned<Checkout<A, O>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let mut parsed = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for path in paths {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let absolute = restore_absolute_path(
                path.trim_start_matches('/'), accounting, cancellation,
            )?;
            let parsed_path = self.mutation_path(&absolute.value, accounting, cancellation)?;
            absolute.retire(accounting)?;
            Self::push_mutation(&mut parsed, parsed_path, accounting)?;
        }
        let source_records = match source_root.value.lookup_batch_no_follow_with_accounting(
            &parsed.value, selected_subtrees, accounting, cancellation,
        ).await {
            Ok((records, allocation)) => {
                let bytes = records.retained_allocation_bytes;
                crate::kernel::Owned::new(records, allocation, bytes)
            }
            Err(error) => {
                source_root.retire(accounting)?;
                return Err(error.into());
            }
        };
        let mut missing_count = 0;
        for entry in &source_records.value.entries {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            missing_count += usize::from(entry.record.is_none());
        }
        let mut missing_paths = crate::kernel::Owned::allocate(missing_count, accounting)?;
        for (path, entry) in parsed.value.iter().zip(&source_records.value.entries) {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if entry.record.is_none() {
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Self::push_mutation(
                    &mut missing_paths, crate::kernel::Owned::new(path, allocation, bytes), accounting,
                )?;
            }
        }
        let current_records = if missing_paths.value.is_empty() {
            None
        } else {
            let (records, allocation) = self.checkout.lookup_batch_no_follow_with_accounting(
                &missing_paths.value, false, accounting, cancellation,
            ).await?;
            let bytes = records.retained_allocation_bytes;
            Some(crate::kernel::Owned::new(records, allocation, bytes))
        };
        if selected_subtrees {
            let mut removal_count = 0;
            if let Some(records) = &current_records {
                for entry in &records.value.entries {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    removal_count += usize::from(entry.record.is_some());
                }
            }
            let mut removals = crate::kernel::Owned::allocate(removal_count, accounting)?;
            if let Some(records) = &current_records {
                for (path, entry) in missing_paths.value.drain(..).zip(&records.value.entries) {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    let bytes = path.retained_bytes();
                    let allocation = crate::kernel::split_allocation(&mut missing_paths.allocation, bytes)?;
                    let path = crate::kernel::Owned::new(path, allocation, bytes);
                    missing_paths.bytes = missing_paths.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
                    if let Some(record) = entry.record {
                        Self::push_mutation(&mut removals, crate::kernel::Owned::new(
                            crate::kernel::Mutation::Remove {
                                path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                            }, path.allocation, path.bytes,
                        ), accounting)?;
                    } else {
                        path.retire(accounting)?;
                    }
                }
            }
            if removals.value.is_empty() {
                removals.retire(accounting)?;
            } else {
                self.checkout.mutate_owned_with_accounting(removals, accounting, cancellation).await?;
            }
        }
        missing_paths.retire(accounting)?;
        let mut current_index = 0;
        let mut operations = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for ((original_path, path), entry) in paths.iter().zip(parsed.value.drain(..))
            .zip(&source_records.value.entries)
        {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let relative = original_path.trim_start_matches('/');
            let bytes = path.retained_bytes();
            let allocation = crate::kernel::split_allocation(&mut parsed.allocation, bytes)?;
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            parsed.bytes = parsed.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
            if let Some(record) = entry.record {
                if selected_subtrees && record.kind == FileKind::Directory {
                    let destination = self.checkout.lookup_no_follow_with_accounting(
                        &path.value, accounting, cancellation, true
                    ).await?.0.record;
                    let absolute = restore_absolute_path(
                        relative, accounting, cancellation,
                    )?;
                    if destination.is_some_and(|record| record.kind != FileKind::Directory) {
                        self.remove_record(&absolute.value, accounting, cancellation).await?;
                    }
                    self.create_dirs(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                if record.kind != FileKind::Directory && let Some((parent, _)) = relative.rsplit_once('/') {
                    let absolute = restore_absolute_path(
                        parent, accounting, cancellation,
                    )?;
                    self.create_dirs(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                Self::push_mutation(&mut operations, crate::kernel::Owned::new(
                    crate::kernel::Mutation::Restore { path: path.value, record },
                    path.allocation, path.bytes,
                ), accounting)?;
            } else {
                let current_record = current_records.as_ref()
                    .and_then(|records| records.value.entries.get(current_index))
                    .ok_or(WorkspaceError::IncompatibleWorkspace)?;
                current_index += 1;
                if !selected_subtrees && let Some(record) = current_record.record {
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(
                        crate::kernel::Mutation::Remove {
                            path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                        }, path.allocation, path.bytes,
                    ), accounting)?;
                } else {
                    path.retire(accounting)?;
                }
            }
        }
        parsed.retire(accounting)?;
        if let Some(records) = current_records {
            records.retire(accounting)?;
        }
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            self.checkout.mutate_owned_with_accounting(operations, accounting, cancellation).await?;
        }
        let records_result = source_records.retire(accounting);
        let root_result = source_root.retire(accounting);
        records_result.and(root_result).map_err(WorkspaceError::from)
    }

}

/// Terminal semantic outcome of an exact workspace-head restoration.
pub enum WorkspaceRestore<G> {
    /// The requested historical generation became current.
    Restored(G),
    /// The same restoration was already durable under this retry identity.
    AlreadyRestored(G),
    /// The requested generation was already current; no publication occurred.
    Current(G),
    /// The workspace advanced after the caller observed it.
    Stale(G),
    /// Workspace writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously bound to another restoration.
    IdempotencyConflict,
}

