use super::*;

/// Immutable complete filesystem state belonging to one workspace.
pub struct Generation<A, O> {
    pub(crate) workspace: Workspace<A, O>,
    pub(crate) id: GenerationId,
    pub(crate) publication_closure: Option<crate::facade::PublicationClosure<A, O>>,
}

impl<A, O> Clone for Generation<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            id: self.id,
            publication_closure: self.publication_closure.clone(),
        }
    }
}

impl<A, O> Generation<A, O> {
    /// Content-addressed generation identity.
    pub const fn id(&self) -> GenerationId {
        self.id
    }

    /// Owning workspace identity.
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace.id
    }

    /// Returns the workspace capability that owns this immutable generation.
    pub fn workspace(&self) -> Workspace<A, O> {
        self.workspace.clone()
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Performs the operation under the supplied limits.
    pub async fn matches_fork_selection<W: crate::facade::FsWork>(
        &self,
        source: &Self,
        paths: Option<Vec<String>>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .verify_fork_selection(source, self, paths, work, cancellation)
            .await
    }

    /// Performs the operation under the supplied limits.
    pub async fn normalized_join_parent_for<W: crate::facade::FsWork>(
        &self,
        target: &Self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<GenerationId, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .workspace
            .volume
            .fs
            .workspace_normalized_join_parent(self, target, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    /// Performs the operation under the supplied limits.
    pub async fn diff_to<W: crate::facade::FsWork>(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(ChangeSet<A, O>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let (changes, allocation) = match &mut accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => {
                    self.workspace
                        .volume
                        .fs
                        .workspace_join_changes(
                            self.id,
                            to,
                            maximum_changes,
                            *original,
                            cancellation,
                        )
                        .await?
                }
                _ => {
                    let result = self
                        .workspace
                        .volume
                        .fs
                        .workspace_join_changes(
                            self.id,
                            to,
                            maximum_changes,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await;
                    let (_, spent, _, _) = accounting
                        .standalone_parts()
                        .ok_or(crate::WorkError::Overflow)?;
                    merge_workspace_backend(result, spent, 0)?.value
                }
            };
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((
                ChangeSet {
                    from: self.clone(),
                    to: to.clone(),
                    changes,
                    work: accounting.spent(),
                },
                allocation,
            ))
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Performs the operation under the supplied limits.
    pub async fn parents(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<GenerationId>>, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .generation_parents(&self.workspace.volume, self.id, budget, cancellation)
            .await
            .map(|receipt| crate::OperationReceipt {
                value: receipt.value.0,
                work: receipt.work,
            })
    }

    /// Reads one complete regular file from this exact generation.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            maximum_bytes,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one exact range from this immutable generation.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
            work,
            cancellation,
        )
        .await
    }

    /// Reads complete no-follow metadata from this immutable generation.
    pub async fn stat<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            work,
            cancellation,
        )
        .await
    }

    /// Resolves an ordered no-follow path batch, preserving absence and duplicates.
    pub async fn lookup_paths(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::FsResult<Vec<Option<crate::kernel::FileRecord>>> {
        let checkout = self
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut accounting = KernelAccounting::Standalone {
            allocations: crate::kernel::allocation::AllocationLedger::default(),
            work: checkout.work,
            budget,
            cancellation,
        };
        let mut checkout = checkout.value;
        let result = async {
            accounting.claim_owned(checkout.logical_owned_bytes(), 0)?;
            let (lookup, allocation) = checkout
                .lookup_batch_no_follow_with_accounting(paths, false, &mut accounting, cancellation)
                .await?;
            let bytes = lookup.retained_allocation_bytes;
            let lookup = crate::kernel::Owned::new(lookup, allocation, bytes);
            let mut records =
                crate::kernel::Owned::allocate(lookup.value.entries.len(), &mut accounting)
                    .map_err(|error| match error {
                        FsError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed) => {
                            FsError::HandleAllocationFailed
                        }
                        error => error,
                    })?;
            for entry in lookup.value.entries {
                accounting.charge(item_work(1))?;
                records.value.push(entry.record);
            }
            Ok(records.value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }

    /// Returns one exact-generation directory page and its result allocation.
    pub async fn list_directory<W: crate::facade::FsWork>(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
        list(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            after,
            maximum_entries,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one opaque symbolic-link target from this exact generation.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            work,
            cancellation,
        )
        .await
    }

    /// Plans clipped sparse spans from this immutable generation.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }

    /// Performs the operation under the supplied limits.
    pub async fn pin<W: crate::facade::FsWork>(
        &self,
        identity: impl AsRef<str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<(WorkspaceName, Generation<A, O>), WorkspaceError> = async {
            let identity = WorkspaceName::with_accounting(identity.as_ref(), &mut accounting)?;
            let mut label = copy_workspace_label(&identity, &mut accounting)?;
            if accounting.standalone_parts().is_some() {
                accounting.release_owned(&mut label.allocation, label.bytes)?;
            }
            let publication = self.publication_closure.as_ref().and_then(|slot| {
                slot.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            });
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .volume
                    .fs
                    .retain_workspace_generation(
                        &self.workspace.volume,
                        self.id,
                        crate::kernel::RetentionKind::Pin,
                        label,
                        publication,
                        original,
                        cancellation,
                    )
                    .await?;
                return Ok((identity, self.clone()));
            }
            let retained = self
                .workspace
                .volume
                .fs
                .retain_workspace_generation(
                    &self.workspace.volume,
                    self.id,
                    crate::kernel::RetentionKind::Pin,
                    label,
                    publication,
                    accounting.remaining()?,
                    cancellation,
                )
                .await;
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            merge_workspace_backend(retained, spent, allocations.live_bytes())?;
            spent.verify(budget)?;
            Ok((identity, self.clone()))
        }
        .await;
        work.finish(result, &accounting)
    }
}
