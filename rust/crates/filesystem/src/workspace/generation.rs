use super::*;

/// Immutable complete filesystem state belonging to one workspace.
pub struct Generation<A: 'static, O: 'static> {
    pub(crate) workspace: Workspace<A, O>,
    pub(crate) id: GenerationId,
    pub(crate) publication_closure: Option<crate::facade::GenerationClosure<A, O>>,
}

impl<A: 'static, O: 'static> Clone for Generation<A, O> {
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

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Generation<A, O> {
    /// Keeps the exact observed proof and original collection registration.
    pub(super) fn observed(
        workspace: &Workspace<A, O>,
        checkout: Checkout<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        let id = checkout.generation_id();
        let publication_closure = checkout.into_generation_closure(accounting)?;
        Ok(Self {
            workspace: workspace.clone(),
            id,
            publication_closure,
        })
    }

    /// Retains this generation until its owning volume ends, reusing durable
    /// published history when available. Unpublished generations receive an
    /// independent pin. Explicit named pins continue to use `pin`.
    pub async fn retain_if_unpublished<W: crate::facade::FsWork>(
        &self,
        identity: impl AsRef<str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = {
            let mut accounting = accounting.allocation_scope();
            async {
                if !self
                    .workspace
                    .volume
                    .fs
                    .workspace_generation_is_retained(
                        &self.workspace.volume,
                        self.id,
                        &mut accounting,
                        cancellation,
                    )
                    .await?
                {
                    self.pin_with_accounting(identity.as_ref(), &mut accounting, cancellation)
                        .await?;
                }
                Ok(())
            }
            .await
        };
        work.finish(result, &accounting)
    }

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

    /// Computes the plaintext digest of the source parent normalized to `target`.
    pub async fn normalized_join_parent_for<W: crate::facade::FsWork>(
        &self,
        target: &Self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::Digest, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .workspace
            .volume
            .fs
            .workspace_normalized_join_parent(self, target, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    /// Opens the shared changed-path frontier over these exact generations.
    /// No complete delta or path list is prepared. Both checkouts stay pinned
    /// until the returned cursor is dropped. The supplied work is the total
    /// budget for admission and all subsequent rows; receipts are cumulative.
    ///
    /// These endpoints describe their authored namespaces. A source-backed
    /// [`crate::LazyWorkspace`] also retains source observations, tombstones and
    /// identity shadows in its [`crate::LazySnapshotRef`]. A query over that
    /// complete cut must resolve those components alongside this frontier.
    /// Without exact source alias coverage, that query must retain
    /// [`crate::LazyWorkspaceError::OriginalSnapshotUnavailable`], rather than
    /// finish a complete page or advance its continuation using authored rows
    /// alone. A source link count or equal authored generations cannot provide
    /// that coverage.
    pub async fn changed_paths_to<W: crate::facade::FsWork>(
        &self,
        to: &Generation<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ChangedPaths<A, O>>, WorkspaceError> {
        ChangedPaths::open(self, to, work, cancellation).await
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
            let (changes, allocation) = self
                .workspace
                .volume
                .fs
                .workspace_join_changes_with_accounting(
                    self.id,
                    to,
                    maximum_changes,
                    &mut accounting,
                    cancellation,
                )
                .await?;
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
            crate::ByteRange { offset, length },
            None,
            work,
            cancellation,
        )
        .await
    }

    /// Reads a range only when its immutable file payload still matches `original`.
    /// Metadata-only changes do not invalidate the content comparison.
    pub async fn read_range_if_unchanged<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        path: &str,
        range: crate::ByteRange,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        if original.workspace.id != self.workspace.id
            || !self
                .workspace
                .volume
                .fs
                .same_deployment(&original.workspace.volume.fs)
        {
            let accounting = work.accounting(cancellation);
            return work.finish(Err(WorkspaceError::ForeignGeneration), &accounting);
        }
        read_range(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            range,
            Some(original.id),
            work,
            cancellation,
        )
        .await
    }

    /// Compares regular-file bytes independently of their storage layout.
    /// Shared payloads and complete chunk hashes avoid body reads; partial chunks
    /// use bounded range comparisons charged to the caller's work budget.
    /// Metadata-only changes do not affect this comparison.
    pub async fn matches_content<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.compare_file(original, path, false, false, work, cancellation)
            .await
    }

    /// Compares the exact file identity, kind and payload without reading bodies.
    /// Both paths must exist; unrelated files and metadata do not affect the result.
    pub async fn matches_file_payload<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.compare_file(original, path, true, false, work, cancellation)
            .await
    }

    /// Compares an optional file's exact identity, kind and payload without
    /// reading bodies. Matching absence is equal; creation or removal differs.
    /// Use `matches_file_payload` when the source must exist.
    pub async fn matches_optional_file_payload<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.compare_file(original, path, true, true, work, cancellation)
            .await
    }

    async fn compare_file<W: crate::facade::FsWork>(
        &self,
        original: &Self,
        path: &str,
        exact_payload: bool,
        allow_absent: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        if original.workspace.id != self.workspace.id
            || !self
                .workspace
                .volume
                .fs
                .same_deployment(&original.workspace.volume.fs)
        {
            let accounting = work.accounting(cancellation);
            return work.finish(Err(WorkspaceError::ForeignGeneration), &accounting);
        }
        matches_content(
            &self.workspace,
            self.id,
            original.id,
            path,
            exact_payload,
            allow_absent,
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
                .lookup_batch_no_follow_with_accounting::<true>(
                    paths,
                    false,
                    &mut accounting,
                    cancellation,
                )
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
                accounting.charge_items(1)?;
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
        let result = self
            .pin_with_accounting(identity.as_ref(), &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(super) async fn pin_with_accounting(
        &self,
        identity: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let mut accounting = accounting.allocation_scope();
        let result = async {
            let identity = WorkspaceName::with_accounting(identity, &mut accounting)?;
            {
                let mut temporary = accounting.allocation_scope();
                let label = copy_workspace_label(&identity, &mut temporary)?;
                let publication = self
                    .publication_closure
                    .as_ref()
                    .and_then(crate::facade::GenerationClosure::take_pin);
                self.workspace
                    .volume
                    .fs
                    .retain_workspace_generation_with_accounting(
                        &self.workspace.volume,
                        self.id,
                        crate::kernel::RetentionKind::Pin,
                        label,
                        publication,
                        &mut temporary,
                        cancellation,
                    )
                    .await?;
            }
            Ok((identity, self.clone()))
        }
        .await;
        if result.is_ok() {
            accounting.keep();
        }
        result
    }
}
