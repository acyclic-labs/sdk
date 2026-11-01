use super::*;

/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A: 'static, O: 'static> {
    pub(super) from: Generation<A, O>,
    pub(super) to: Generation<A, O>,
    pub(super) changes: GenerationDiff,
    pub(super) work: crate::WorkCounters,
}

pub use crate::kernel::changed_paths::ChangedPath;

impl<A, O> ChangeSet<A, O> {
    /// Exact immutable base endpoint.
    pub const fn from(&self) -> &Generation<A, O> {
        &self.from
    }

    /// Exact immutable resulting endpoint.
    pub const fn to(&self) -> &Generation<A, O> {
        &self.to
    }

    /// Stable path-independent records and namespace binding changes.
    pub const fn changes(&self) -> &GenerationDiff {
        &self.changes
    }

    /// Exact bounded work used to authenticate and compute this delta.
    pub const fn work(&self) -> crate::WorkCounters {
        self.work
    }

    /// Retained diff buffer bytes.
    pub fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.changes.files)
            + workspace_vec_bytes(&self.changes.bindings)
            + self
                .changes
                .bindings
                .iter()
                .map(|binding| {
                    crate::foundation::usize_to_u64(binding.name.retained_bytes())
                        + binding.before.as_ref().map_or(0, |entry| {
                            crate::foundation::usize_to_u64(entry.name.retained_bytes())
                        })
                        + binding.after.as_ref().map_or(0, |entry| {
                            crate::foundation::usize_to_u64(entry.name.retained_bytes())
                        })
                })
                .sum::<u64>()
    }
}

/// Complete path delta from the same retained producer used by paged queries.
pub(crate) async fn complete_changed_paths<O: AsyncObjectStore>(
    endpoints: [Option<super::namespace_view::NamespaceView<'_, O>>; 2],
    maximum_entries: impl Into<Option<u32>>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let Some(view) = endpoints.iter().flatten().next() else {
        return Ok((Vec::new(), accounting.claim_owned(0, 0)?));
    };
    let roots = endpoints
        .each_ref()
        .map(|view| view.as_ref().map(|view| view.root));
    let cursor = crate::kernel::changed_paths::Cursor::new(roots, view.config, accounting)?;
    collect_paths(
        cursor,
        view.objects,
        roots,
        maximum_entries,
        accounting,
        cancellation,
    )
    .await
}

async fn collect_paths<O: AsyncObjectStore>(
    mut cursor: crate::kernel::changed_paths::Cursor<'_>,
    objects: &O,
    roots: [Option<&crate::kernel::GenerationRoot>; 2],
    maximum_entries: impl Into<Option<u32>>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let mut paths = match crate::kernel::Owned::<Vec<ChangedPath>>::allocate(0, accounting) {
        Ok(paths) => paths,
        Err(error) => {
            cursor.retire(accounting)?;
            return Err(error.into());
        }
    };
    let maximum = maximum_entries.into().map(|maximum| maximum as usize);
    let result = async {
        while let Some(path) = cursor
            .next(objects, roots, accounting, cancellation)
            .await?
        {
            if maximum == Some(paths.value.len()) {
                path.retire(accounting)?;
                return Err(WorkspaceError::ChangedPathLimit);
            }
            let prepared = (|| {
                let moved = if paths.value.len() == paths.value.capacity() {
                    paths.value.len()
                } else {
                    0
                };
                let copied = copy_work(crate::foundation::usize_to_u64(
                    moved
                        .checked_add(1)
                        .and_then(|count| count.checked_mul(std::mem::size_of::<ChangedPath>()))
                        .ok_or(crate::WorkError::Overflow)?,
                ));
                accounting.admit(copied)?;
                paths.ensure_for_push(maximum.unwrap_or(usize::MAX), accounting)?;
                accounting.record_completed(copied)
            })();
            if let Err(error) = prepared {
                path.retire(accounting)?;
                return Err(error.into());
            }
            let path = path.transfer_to(&mut paths.allocation, &mut paths.bytes, accounting)?;
            paths.value.push(path);
        }
        // The public vector API retains its historical path order. The cursor
        // itself emits each path once, so no prefix index or coalescing is needed.
        crate::kernel::sort_by_with_accounting(&mut paths.value, accounting, |left, right| {
            left.path.components().cmp(right.path.components())
        })?;
        Ok(())
    }
    .await;
    let retirement = cursor.retire(accounting);
    match result.and_then(|()| retirement.map_err(Into::into)) {
        Ok(()) => Ok((paths.value, paths.allocation)),
        Err(error) => {
            paths.retire(accounting)?;
            Err(error)
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ChangeSet<A, O> {
    /// Resolves changed paths while retaining their actual result allocation.
    pub async fn changed_paths<W: crate::facade::FsWork>(
        &self,
        maximum_entries: impl Into<Option<u32>>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(Vec<ChangedPath>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if let Some((allocations, spent, budget, _)) = accounting.standalone_parts() {
                *spent = self.work;
                spent.verify(budget)?;
                allocations
                    .claim_bytes(self.retained_bytes(), 0, spent, budget)
                    .map_err(workspace_allocation_error)?;
            }
            let (paths, allocation) = self
                .changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation)
                .await?;
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((paths, allocation))
        }
        .await;
        work.finish(result, &accounting)
    }

    async fn changed_paths_with_accounting(
        &self,
        maximum_entries: impl Into<Option<u32>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        if self.changes.truncated {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let from = self
            .from
            .workspace
            .open_checkout(
                GenerationSelector::Exact(self.from.id),
                CheckoutMode::read_only_pinned(),
                accounting,
                cancellation,
            )
            .await?;
        let to = match self
            .to
            .workspace
            .open_checkout(
                GenerationSelector::Exact(self.to.id),
                CheckoutMode::read_only_pinned(),
                accounting,
                cancellation,
            )
            .await
        {
            Ok(to) => to,
            Err(error) => {
                from.retire(accounting)?;
                return Err(error);
            }
        };
        let result = async {
            let roots = [Some(from.value.root()), Some(to.value.root())];
            let cursor = crate::kernel::changed_paths::Cursor::from_records(
                roots,
                self.from.workspace.volume.config(),
                &self.changes.files,
            )?;
            collect_paths(
                cursor,
                self.from.workspace.volume.fs.objects(),
                roots,
                maximum_entries,
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        let to = to.retire(accounting);
        let from = from.retire(accounting);
        match (result, to.and(from)) {
            (Ok(paths), Ok(())) => Ok(paths),
            (Ok(paths), Err(error)) => {
                let bytes = workspace_vec_bytes(&paths.0)
                    + paths
                        .0
                        .iter()
                        .map(|row| row.path.retained_bytes())
                        .sum::<u64>();
                reverse_drop(paths, bytes, accounting)?;
                Err(error.into())
            }
            (Err(error), _) => Err(error),
        }
    }

    /// Computes the bounded net delta of two contiguous change sets.
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: impl Into<Option<u32>>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ChangeSet<A, O>>, WorkspaceError> {
        if self.to.id != next.from.id
            || self.to.workspace.id != next.from.workspace.id
            || !self
                .to
                .workspace
                .volume
                .fs
                .same_deployment(&next.from.workspace.volume.fs)
        {
            return Err(crate::OperationFailure::before_work(
                WorkspaceError::ChangeSetContinuity,
            ));
        }
        self.from
            .workspace
            .diff(&self.from, &next.to, maximum_changes, budget, cancellation)
            .await
    }
}
