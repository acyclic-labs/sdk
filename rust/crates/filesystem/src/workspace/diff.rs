use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Computes one immutable bounded semantic change set between two exact
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if from.workspace.id != self.id || to.workspace.id != self.id
            || !self.volume.fs.same_deployment(&from.workspace.volume.fs)
            || !self.volume.fs.same_deployment(&to.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let receipt = self
            .volume
            .diff_generations(
                from.id,
                to.id,
                maximum_changes,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        if receipt.value.truncated {
            return Err(WorkspaceError::JoinLimit);
        }
        Ok(ChangeSet {
            from: from.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }
}

/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A, O> {
    from: Generation<A, O>,
    to: Generation<A, O>,
    changes: GenerationDiff,
    work: crate::WorkCounters,
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
    #[must_use]
    pub const fn from(&self) -> &Generation<A, O> {
        &self.from
    }

    /// Exact immutable resulting endpoint.
    #[must_use]
    pub const fn to(&self) -> &Generation<A, O> {
        &self.to
    }

    /// Stable path-independent records and namespace binding changes.
    #[must_use]
    pub const fn changes(&self) -> &GenerationDiff {
        &self.changes
    }

    /// Exact bounded work used to authenticate and compute this delta.
    #[must_use]
    pub const fn work(&self) -> crate::WorkCounters {
        self.work
    }
    /// Actual retained diff buffers, excluding shared immutable endpoints.
    pub fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.changes.files)
            + workspace_vec_bytes(&self.changes.bindings)
            + self.changes.bindings.iter().map(|binding| {
                crate::foundation::usize_to_u64(binding.name.retained_bytes())
                    + binding.before.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
                    + binding.after.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
            }).sum::<u64>()
    }
}

fn changed_path_position<'a>(
    paths: &[ChangedPath], components: impl Iterator<Item = &'a LogicalName> + Clone,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(usize, bool), WorkspaceError> {
    bounded_search(paths, accounting, |path, accounting| {
        let mut left = path.path.components().iter();
        let mut right = components.clone();
        loop {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            match (left.next(), right.next()) {
                (Some(left), Some(right)) => match left.cmp(right) {
                    std::cmp::Ordering::Equal => {}
                    ordering => return Ok(ordering),
                },
                (Some(_), None) => return Ok(std::cmp::Ordering::Greater),
                (None, Some(_)) => return Ok(std::cmp::Ordering::Less),
                (None, None) => return Ok(std::cmp::Ordering::Equal),
            }
        }
    }).map_err(WorkspaceError::from)
}

fn set_changed_record(
    paths: &mut [ChangedPath], index: usize, record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    if before { paths[index].before = record; } else { paths[index].after = record; }
    accounting.record_completed(copied)?;
    Ok(())
}

fn insert_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), index: usize,
    mut path: (NamespacePath, KernelOwnedAllocation), record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: ((paths.0.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    paths.1.absorb(&mut path.1)?;
    paths.0.insert(index, ChangedPath {
        path: path.0, before: if before { record } else { None },
        after: if before { None } else { record },
    });
    accounting.record_completed(copied)?;
    Ok(())
}

fn merge_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let (index, present) = changed_path_position(&paths.0, path.components().iter(), accounting)?;
    if present { return set_changed_record(&mut paths.0, index, record, before, accounting); }
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    insert_changed_record(paths, index, (path, allocation), record, before, accounting)
}

fn merge_binding_endpoint(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), directories: &NamespaceRecords,
    change: &crate::DirectoryBindingChange, limits: crate::model::VolumeLimits, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let Some(parents) = bounded_search(directories, accounting, |record, _| Ok(record.0.cmp(&change.directory_id))).map(|(index, present)| present.then(|| &directories[index].1))?
        else { return Ok(()); };
    let binding = if before { change.before.as_ref() } else { change.after.as_ref() };
    let record = match binding {
        Some(entry) => bounded_search(directories, accounting, |record, _| Ok(record.0.cmp(&entry.file_id))).map(|(index, present)| present.then(|| &directories[index].1))?
            .and_then(|records| records.first().map(|(_, record)| *record)),
        None => None,
    };
    for (directory, _) in parents {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let components = directory.components().iter().chain(std::iter::once(&change.name));
        let (index, present) = changed_path_position(&paths.0, components, accounting)?;
        if present {
            set_changed_record(&mut paths.0, index, record, before, accounting)?;
        } else {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            let (path, allocation) = directory.child_with_accounting(
                &change.name, limits, accounting, cancellation,
            )?;
            insert_changed_record(paths, index, (path, allocation), record, before, accounting)?;
        }
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves changed paths while retaining their actual result allocation.
    pub async fn changed_paths<W: crate::facade::FsWork>(
        &self, maximum_entries: u32, work: W, cancellation: &CancellationToken,
    ) -> W::Output<(Vec<ChangedPath>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if let Some((allocations, spent, budget, _)) = accounting.standalone_parts() {
                *spent = self.work;
                spent.verify(budget)?;
                allocations.claim_bytes(self.retained_bytes(), 0, spent, budget).map_err(workspace_allocation_error)?;
            }
            let (paths, allocation) = self.changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation).await?;
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((paths, allocation))
        }.await;
        work.finish(result, &accounting)
    }

    async fn changed_paths_with_accounting(
        &self, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if self.changes.truncated || (maximum_entries == 0
            && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty())) {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids = sorted_changed_file_ids(
            self.changes.files.iter().map(|change| change.file_id)
                .chain(self.changes.bindings.iter().map(|change| change.directory_id))
                .chain(self.changes.bindings.iter().flat_map(|change| {
                    change.before.iter().chain(change.after.iter()).map(|entry| entry.file_id)
                })), accounting,
        )?;
        let before = self.from.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        let after = self.to.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        if !before.0.complete || !after.0.complete { return Err(WorkspaceError::ChangedPathLimit); }
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        for records in before.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), true, accounting, cancellation)?;
            }
        }
        for records in after.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), false, accounting, cancellation)?;
            }
        }
        for change in &self.changes.bindings {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            merge_binding_endpoint(&mut paths, &before.0.records, change, self.from.workspace.volume.config.limits, true, accounting, cancellation)?;
            merge_binding_endpoint(&mut paths, &after.0.records, change, self.to.workspace.volume.config.limits, false, accounting, cancellation)?;
        }
        let before_bytes = namespace_records_bytes(&before.0.records);
        let after_bytes = namespace_records_bytes(&after.0.records);
        let id_bytes = workspace_vec_bytes(&file_ids.0);
        reverse_drop(before, before_bytes, accounting)?;
        reverse_drop(after, after_bytes, accounting)?;
        reverse_drop(file_ids, id_bytes, accounting)?;
        let mut retained = 0;
        let mut retired_paths = 0_u64;
        for index in 0..paths.0.len() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let changed = match (&paths.0[index].before, &paths.0[index].after) {
                (Some(before), Some(after)) => before.kind != after.kind || before.metadata != after.metadata
                    || (before.kind != FileKind::Directory && before.payload != after.payload),
                (None, None) => false,
                _ => true,
            };
            if changed {
                if retained != index {
                    let copied = crate::WorkCounters { bytes_copied: (2 * std::mem::size_of::<ChangedPath>()) as u64, ..crate::WorkCounters::UNCHARGED };
                    accounting.admit(copied)?;
                    paths.0.swap(retained, index);
                    accounting.record_completed(copied)?;
                }
                retained += 1;
            } else {
                retired_paths = retired_paths.checked_add(paths.0[index].path.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
            }
        }
        paths.0.truncate(retained);
        accounting.release_owned(&mut paths.1, retired_paths)?;
        accounting.check()?;
        Ok(paths)
    }

    /// Composes contiguous immutable deltas into their exact net semantic
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if self.to.id != next.from.id
            || self.to.workspace.id != next.from.workspace.id
            || !self
                .to
                .workspace
                .volume
                .fs
                .same_deployment(&next.from.workspace.volume.fs)
        {
            return Err(WorkspaceError::ChangeSetContinuity);
        }
        self.from
            .workspace
            .diff(&self.from, &next.to, maximum_changes)
            .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Computes a bounded semantic delta retaining its actual result allocation.
    pub async fn diff_to<W: crate::facade::FsWork>(
        &self, to: &Generation<A, O>, maximum_changes: u32, work: W, cancellation: &CancellationToken,
    ) -> W::Output<(ChangeSet<A, O>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let (changes, allocation) = match &mut accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => self.workspace.volume.fs
                    .workspace_join_changes(self.id, to, maximum_changes, *original, cancellation).await?,
                _ => {
                    let result = self.workspace.volume.fs.workspace_join_changes(self.id, to, maximum_changes, accounting.remaining()?, cancellation).await;
                    let (_, spent, _, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
                    merge_workspace_backend(result, spent, 0)?.value
                }
            };
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((ChangeSet { from: self.clone(), to: to.clone(), changes, work: accounting.spent() }, allocation))
        }.await;
        work.finish(result, &accounting)
    }
}
