use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Restores one exact generation only while the supplied writer permit remains valid.
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
        let restored = |id| Generation {
            workspace: self.clone(),
            id,
            publication_closure: None,
        };
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

    /// Restores exact records and prepares selected directories before one publication seal.
    pub async fn restore_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(
            move |budget| async move {
                let mut accounting = KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                        return Err(WorkspaceError::ForeignGeneration);
                    }
                    let id = self
                        .checkout_accounted(
                            GenerationSelector::Head,
                            CheckoutMode::read_only_pinned(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?
                        .generation_id();
                    if id != if_current {
                        return Ok(TransactionCommit::Conflict {
                            actual: Generation {
                                workspace: self.clone(),
                                id,
                                publication_closure: None,
                            },
                        });
                    }
                    let source_root = source
                        .workspace
                        .open_checkout(
                            GenerationSelector::Exact(source.id),
                            CheckoutMode::read_only_pinned(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let opened = self
                        .open_checkout(
                            GenerationSelector::Exact(id),
                            CheckoutMode::tracking_transaction(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let mut transaction =
                        Transaction::from_checkout(self, opened.value, idempotency_key, false);
                    transaction
                        .restore_paths_from_root_with_accounting(
                            paths,
                            source_root,
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let committed = transaction
                        .commit(
                            None,
                            permit,
                            accounting.remaining()?,
                            cancellation,
                            |budget| {
                                let future = before_publication(budget);
                                async move {
                                    future
                                        .await
                                        .map_err(|failure| failure.map_error(WorkspaceError::from))
                                }
                            },
                        )
                        .await;
                    let (allocations, spent, _, _) = accounting
                        .standalone_parts()
                        .ok_or(crate::WorkError::Overflow)?;
                    let live = allocations.live_bytes();
                    merge_workspace_backend(committed, spent, live).map(|receipt| receipt.value)
                }
                .await;
                accounting.check().map_err(|error| {
                    crate::OperationFailure::new(WorkspaceError::from(error), accounting.spent())
                })?;
                crate::facade::measured(result, &accounting)
            },
            budget,
            cancellation,
            WorkspaceError::Cancelled(crate::CancellationError),
            None,
        )
        .await
    }

    /// Restores the caller-expanded frozen selection at the original native seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn restore_paths_from_with_native_work<'original, E, F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
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
        let mut transaction = self
            .begin_pinned_transaction(&current, idempotency_key, original, cancellation)
            .await
            .map_err(E::from)?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        transaction
            .checkout
            .validate_native_work(original)
            .map_err(FsError::from)?;
        let source_root = source
            .workspace
            .open_checkout(
                GenerationSelector::Exact(source.id),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(E::from)?;
        transaction
            .restore_paths_from_root_with_accounting(
                paths,
                source_root,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(E::from)?;
        transaction
            .commit_with_native_work(None, permit, original, cancellation, before_publication)
            .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    async fn restore_paths_from_root_with_accounting(
        &mut self,
        paths: &[String],
        mut source_root: crate::kernel::Owned<Checkout<A, O>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let mut parsed = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for path in paths {
            accounting.charge(item_work(1))?;
            let absolute =
                restore_absolute_path(path.trim_start_matches('/'), accounting, cancellation)?;
            let path = self.mutation_path(&absolute.value, accounting, cancellation)?;
            absolute.retire(accounting)?;
            Self::push_mutation(&mut parsed, path, accounting)?;
        }
        let (records, allocation) = source_root
            .value
            .lookup_batch_no_follow_with_accounting(&parsed.value, true, accounting, cancellation)
            .await?;
        let bytes = records.retained_allocation_bytes;
        let records = crate::kernel::Owned::new(records, allocation, bytes);
        let mut removals = crate::kernel::Owned::allocate(0, accounting)?;
        for (path, entry) in parsed.value.iter().zip(&records.value.entries) {
            accounting.charge(item_work(1))?;
            if entry.record.is_some() {
                continue;
            }
            let current = self
                .checkout
                .lookup_no_follow_with_accounting(path, accounting, cancellation, false)
                .await?
                .0
                .record;
            if let Some(record) = current {
                removals.ensure_for_push(paths.len(), accounting)?;
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Self::push_mutation(
                    &mut removals,
                    crate::kernel::Owned::new(
                        crate::kernel::Mutation::Remove {
                            path,
                            expected_file_id: MetadataField::Value(record.file_id),
                        },
                        allocation,
                        bytes,
                    ),
                    accounting,
                )?;
            }
        }
        if removals.value.is_empty() {
            removals.retire(accounting)?;
        } else {
            self.checkout
                .mutate_owned_with_accounting(removals, accounting, cancellation)
                .await?;
        }
        let mut operations = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for ((original, path), entry) in paths
            .iter()
            .zip(parsed.value.drain(..))
            .zip(&records.value.entries)
        {
            accounting.charge(item_work(1))?;
            let bytes = path.retained_bytes();
            let allocation = crate::kernel::split_allocation(&mut parsed.allocation, bytes)?;
            parsed.bytes = parsed
                .bytes
                .checked_sub(bytes)
                .ok_or(crate::WorkError::Overflow)?;
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            let Some(record) = entry.record else {
                path.retire(accounting)?;
                continue;
            };
            let relative = original.trim_start_matches('/');
            let directory = if record.kind == FileKind::Directory {
                let current = self
                    .checkout
                    .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
                    .await?
                    .0
                    .record;
                if current.is_some_and(|record| record.kind != FileKind::Directory) {
                    let absolute = restore_absolute_path(relative, accounting, cancellation)?;
                    self.remove_record(&absolute.value, accounting, cancellation)
                        .await?;
                    absolute.retire(accounting)?;
                }
                Some(relative)
            } else {
                relative.rsplit_once('/').map(|(parent, _)| parent)
            };
            if let Some(directory) = directory {
                let absolute = restore_absolute_path(directory, accounting, cancellation)?;
                self.create_dirs(&absolute.value, accounting, cancellation)
                    .await?;
                absolute.retire(accounting)?;
            }
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(
                    crate::kernel::Mutation::Restore {
                        path: path.value,
                        record,
                    },
                    path.allocation,
                    path.bytes,
                ),
                accounting,
            )?;
        }
        parsed.retire(accounting)?;
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            self.checkout
                .mutate_owned_with_accounting(operations, accounting, cancellation)
                .await?;
        }
        let records_result = records.retire(accounting);
        let root_result = source_root.retire(accounting);
        records_result
            .and(root_result)
            .map_err(WorkspaceError::from)
    }
}
