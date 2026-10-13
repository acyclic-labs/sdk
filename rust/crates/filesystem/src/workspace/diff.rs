use super::*;

/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A, O> {
    pub(super) from: Generation<A, O>,
    pub(super) to: Generation<A, O>,
    pub(super) changes: GenerationDiff,
    pub(super) work: crate::WorkCounters,
}

/// One changed exact path between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedPath {
    /// Canonical exact path in either endpoint.
    pub path: NamespacePath,
    /// Complete path-independent record in the earlier generation, when present.
    pub before: Option<crate::kernel::FileRecord>,
    /// Complete path-independent record in the later generation, when present.
    pub after: Option<crate::kernel::FileRecord>,
}

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

fn merge_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    match binary_search_with_accounting(&paths.0, accounting, |entry| {
        entry.path.components().cmp(path.components())
    })? {
        Ok(index) => {
            let copied = copy_work(std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64);
            accounting.admit(copied)?;
            if before {
                paths.0[index].before = record;
            } else {
                paths.0[index].after = record;
            }
            accounting.record_completed(copied)?;
        }
        Err(index) => {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            let (path, mut allocation) = path.clone_with_accounting(accounting, cancellation)?;
            let copied = copy_work(
                ((paths.0.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
            );
            accounting.admit(copied)?;
            paths.1.absorb(&mut allocation)?;
            paths.0.insert(
                index,
                ChangedPath {
                    path,
                    before: if before { record } else { None },
                    after: if before { None } else { record },
                },
            );
            accounting.record_completed(copied)?;
        }
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves changed paths while retaining their actual result allocation.
    pub async fn changed_paths<W: crate::facade::FsWork>(
        &self,
        maximum_entries: u32,
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
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if self.changes.truncated
            || (maximum_entries == 0
                && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty()))
        {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids = sorted_changed_file_ids(
            self.changes
                .files
                .iter()
                .map(|change| change.file_id)
                .chain(self.changes.bindings.iter().flat_map(|change| {
                    change
                        .before
                        .iter()
                        .chain(change.after.iter())
                        .map(|entry| entry.file_id)
                })),
            accounting,
        )?;
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        for (generation, before) in [(&self.from, true), (&self.to, false)] {
            let search = generation
                .namespace_records_with_accounting(
                    &file_ids.0,
                    maximum_entries,
                    accounting,
                    cancellation,
                )
                .await?;
            if !search.0.complete {
                return Err(WorkspaceError::ChangedPathLimit);
            }
            for (_, records) in &search.0.records {
                for (path, record) in records {
                    accounting.charge(item_work(1))?;
                    merge_changed_record(
                        &mut paths,
                        path,
                        Some(*record),
                        before,
                        accounting,
                        cancellation,
                    )?;
                }
            }
            let bytes = namespace_records_bytes(&search.0.records);
            reverse_drop(search, bytes, accounting)?;
        }
        let id_bytes = workspace_vec_bytes(&file_ids.0);
        reverse_drop(file_ids, id_bytes, accounting)?;
        let mut retained = 0;
        let mut retired_paths = 0_u64;
        for index in 0..paths.0.len() {
            accounting.charge(item_work(1))?;
            let changed = match (&paths.0[index].before, &paths.0[index].after) {
                (Some(before), Some(after)) => {
                    before.kind != after.kind
                        || before.metadata != after.metadata
                        || (before.kind != FileKind::Directory && before.payload != after.payload)
                }
                (None, None) => false,
                _ => true,
            };
            if changed {
                if retained != index {
                    let copied = copy_work((2 * std::mem::size_of::<ChangedPath>()) as u64);
                    accounting.admit(copied)?;
                    paths.0.swap(retained, index);
                    accounting.record_completed(copied)?;
                }
                retained += 1;
            } else {
                retired_paths = retired_paths
                    .checked_add(paths.0[index].path.retained_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
            }
        }
        paths.0.truncate(retained);
        accounting.release_owned(&mut paths.1, retired_paths)?;
        accounting.check()?;
        Ok(paths)
    }

    /// Computes the bounded net delta of two contiguous change sets.
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: u32,
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
