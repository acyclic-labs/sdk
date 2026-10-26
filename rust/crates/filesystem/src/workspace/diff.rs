use super::*;

mod created;
mod moved;
mod paths;

/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A: 'static, O: 'static> {
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
            let entry = paths.0.get_mut(index).ok_or(FsError::InvalidDiff)?;
            if before {
                entry.before = record;
            } else {
                entry.after = record;
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

trait NamespaceEndpoints: acyclic_stream::ProviderPlatform {
    fn search(
        &self,
        before: bool,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError>>
    + ProviderTask;
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> NamespaceEndpoints
    for ChangeSet<A, O>
{
    async fn search(
        &self,
        before: bool,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        let generation = if before { &self.from } else { &self.to };
        generation
            .namespace_records_with_accounting(ids, maximum_entries, accounting, cancellation)
            .await
    }
}

impl<O: AsyncObjectStore> NamespaceEndpoints for [super::namespace_view::NamespaceView<'_, O>; 2] {
    async fn search(
        &self,
        before: bool,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        let [from, to] = self;
        let view = if before { from } else { to };
        view.records(ids, maximum_entries, accounting, cancellation)
            .await
            .map_err(Into::into)
    }
}

/// Complete path delta, including descendants whose unchanged file records move
/// when an ancestor is renamed. The supplied scan bound never truncates an
/// accepted publication's visible change list.
pub(crate) async fn complete_changed_paths<O: AsyncObjectStore>(
    endpoints: [Option<super::namespace_view::NamespaceView<'_, O>>; 2],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    if let [Some(from), Some(to)] = endpoints {
        return complete_between([from, to], maximum_entries, accounting, cancellation).await;
    }
    collect_all_paths(endpoints, maximum_entries, accounting, cancellation).await
}

pub(super) async fn collect_all_paths<O: AsyncObjectStore>(
    endpoints: [Option<super::namespace_view::NamespaceView<'_, O>>; 2],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
    for (index, view) in endpoints.into_iter().enumerate() {
        let Some(view) = view else {
            continue;
        };
        let search = view
            .all_records(maximum_entries, accounting, cancellation)
            .await?;
        if !search.0.complete {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        for (_, records) in &search.0.records {
            for (path, record) in records {
                accounting.charge_items(1)?;
                paths::append(
                    &mut paths,
                    path,
                    Some(*record),
                    index == 0,
                    accounting,
                    cancellation,
                )?;
            }
        }
        let bytes = namespace_records_bytes(&search.0.records);
        reverse_drop(search, bytes, accounting)?;
    }
    paths::sort_and_coalesce(&mut paths, accounting)?;
    retain_changed_paths(paths, accounting)
}

async fn complete_between<O: AsyncObjectStore>(
    endpoints: [super::namespace_view::NamespaceView<'_, O>; 2],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let [from, to] = &endpoints;
    let changes = crate::kernel::diff_generation_file_tables(
        to.objects,
        from.root.file_table,
        to.root.file_table,
        maximum_entries,
        crate::kernel::DecodeLimits::for_volume(to.config),
        accounting,
        cancellation,
    )
    .await?;
    if changes.0.truncated {
        return Err(WorkspaceError::ChangedPathLimit);
    }
    // A binding of an existing directory can move unchanged descendants. New
    // directories have all of their records in the Merkle delta already.
    let moves_directory = moves_existing_directory(&changes.0, accounting)?;
    let result = if moves_directory {
        moved::expand(
            &endpoints,
            &changes.0,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await
    } else {
        match created::created_paths(
            &endpoints,
            &changes.0,
            maximum_entries,
            to.config.limits,
            accounting,
            cancellation,
        )
        .await?
        {
            Some(paths) => Ok(paths),
            None => {
                resolve_changed_paths(
                    &endpoints,
                    &changes.0,
                    maximum_entries,
                    accounting,
                    cancellation,
                )
                .await
            }
        }
    };
    let bytes = workspace_vec_bytes(&changes.0.files)
        + workspace_vec_bytes(&changes.0.bindings)
        + changes
            .0
            .bindings
            .iter()
            .map(|binding| {
                binding.name.retained_bytes() as u64
                    + binding
                        .before
                        .as_ref()
                        .map_or(0, |entry| entry.name.retained_bytes() as u64)
                    + binding
                        .after
                        .as_ref()
                        .map_or(0, |entry| entry.name.retained_bytes() as u64)
            })
            .sum::<u64>();
    reverse_drop(changes, bytes, accounting)?;
    result
}

fn moves_existing_directory(
    changes: &GenerationDiff,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, WorkspaceError> {
    for binding in &changes.bindings {
        for entry in binding.before.iter().chain(binding.after.iter()) {
            accounting.charge_items(1)?;
            if entry.kind != FileKind::Directory {
                continue;
            }
            let position = binary_search_with_accounting(&changes.files, accounting, |file| {
                file.file_id.cmp(&entry.file_id)
            })?;
            let newly_created = position
                .ok()
                .and_then(|index| changes.files.get(index))
                .is_some_and(|file| file.before.is_none());
            if !newly_created {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn resolve_changed_paths(
    endpoints: &impl NamespaceEndpoints,
    changes: &GenerationDiff,
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    accounting.check()?;
    if changes.truncated
        || (maximum_entries == 0 && (!changes.files.is_empty() || !changes.bindings.is_empty()))
    {
        return Err(WorkspaceError::ChangedPathLimit);
    }
    let file_ids =
        sorted_changed_file_ids(
            changes.files.iter().map(|change| change.file_id).chain(
                changes.bindings.iter().flat_map(|change| {
                    change
                        .before
                        .iter()
                        .chain(change.after.iter())
                        .map(|entry| entry.file_id)
                }),
            ),
            accounting,
        )?;
    let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
    for before in [true, false] {
        let search = endpoints
            .search(
                before,
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
                accounting.charge_items(1)?;
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
    retain_changed_paths(paths, accounting)
}

fn record_changed(before: &crate::kernel::FileRecord, after: &crate::kernel::FileRecord) -> bool {
    before.file_id != after.file_id
        || before.kind != after.kind
        || before.metadata != after.metadata
        || (before.kind != FileKind::Directory && before.payload != after.payload)
}

fn retain_changed_paths(
    mut paths: (Vec<ChangedPath>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let mut retained = 0;
    let mut retired_paths = 0_u64;
    for index in 0..paths.0.len() {
        accounting.charge_items(1)?;
        let entry = paths.0.get(index).ok_or(FsError::InvalidDiff)?;
        let changed = match (&entry.before, &entry.after) {
            (Some(before), Some(after)) => record_changed(before, after),
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
                .checked_add(entry.path.retained_bytes())
                .ok_or(crate::WorkError::Overflow)?;
        }
    }
    paths.0.truncate(retained);
    accounting.release_owned(&mut paths.1, retired_paths)?;
    accounting.check()?;
    Ok(paths)
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ChangeSet<A, O> {
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
        if self.changes.truncated {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        // The output bound includes unchanged descendants of moved directories;
        // only those changed bindings are expanded on the original ledger.
        let result = if moves_existing_directory(&self.changes, accounting)? {
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
            let to = self
                .to
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.to.id),
                    CheckoutMode::read_only_pinned(),
                    accounting,
                    cancellation,
                )
                .await?;
            let view = |root| super::namespace_view::NamespaceView {
                objects: self.from.workspace.volume.fs.objects(),
                config: self.from.workspace.volume.config(),
                root,
            };
            let result = moved::expand(
                &[view(from.value.root()), view(to.value.root())],
                &self.changes,
                u32::MAX,
                accounting,
                cancellation,
            )
            .await;
            to.retire(accounting)?;
            from.retire(accounting)?;
            result?
        } else {
            resolve_changed_paths(self, &self.changes, u32::MAX, accounting, cancellation).await?
        };
        if result.0.len() > maximum_entries as usize {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        Ok(result)
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
