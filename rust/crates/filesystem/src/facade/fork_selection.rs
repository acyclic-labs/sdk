//! Immutable namespace selection using the caller's original accounting owner.

use super::*;
use crate::kernel::{
    FileTableMutation, TreeEntry, TreeMutation, Owned,
    apply_file_table_mutations_with_accounting, apply_tree_mutations_with_accounting,
    encode_file_table_page_with_accounting, encode_tree_page_with_accounting,
    list_tree_entries_async_with_accounting, lookup_file_record_async_with_accounting,
    put_encoded_with_accounting,
};
use crate::kernel::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use crate::workspace::{
    WorkspaceError, customer_path_with_accounting, workspace_owned_strings_bytes,
    workspace_remaining, workspace_vec_bytes,
};

struct ForkDirectorySelection {
    path: Option<usize>,
    depth: usize,
    children: Owned<Vec<usize>>,
}

fn fork_selection_merge_work(
    work: &mut WorkCounters, next: WorkCounters, live: u64, budget: WorkBudget,
) -> Result<(), WorkspaceError> {
    *work = work.with_backend(next, live).map_err(|error| WorkspaceError::WorkMerge {
        prior: *work, backend: next, error,
    })?;
    work.verify_ref(&budget)?;
    Ok(())
}

fn fork_selection_failure<E>(
    failure: OperationFailure<E>, work: &mut WorkCounters, live: u64,
    map: impl FnOnce(E) -> WorkspaceError,
) -> WorkspaceError {
    match work.with_backend(*failure.work, live) {
        Ok(actual) => { *work = actual; map(failure.error) }
        Err(error) => WorkspaceError::WorkMerge {
            prior: *work, backend: *failure.work, error,
        },
    }
}

fn fork_selection_allocation(error: crate::kernel::allocation::AllocationError) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

fn fork_selection_compare_names(
    left: &[LogicalName], right: &[LogicalName], accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<std::cmp::Ordering, WorkspaceError> {
    fork_selection_items(accounting, cancellation)?;
    for (left, right) in left.iter().zip(right) {
        fork_selection_items(accounting, cancellation)?;
        let order = left.cmp(right);
        if !order.is_eq() { return Ok(order); }
    }
    Ok(left.len().cmp(&right.len()))
}

fn fork_directory_components<'a>(
    directory: &ForkDirectorySelection, selected: &'a [Owned<NamespacePath>],
) -> Result<&'a [LogicalName], WorkspaceError> {
    match directory.path {
        None => Ok(&[]),
        Some(index) => selected.get(index)
            .and_then(|path| path.value.components().get(..directory.depth))
            .ok_or(WorkspaceError::JoinLimit),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn verify_fork_selection(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
    ) -> Result<bool, WorkspaceError> {
        self.verify_fork_selection_measured(
            source, initial, paths, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    pub(crate) async fn verify_fork_selection_measured(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredReceipt<bool, WorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        let result = async {
            let input_bytes = paths.as_ref().map(workspace_owned_strings_bytes).transpose()?.unwrap_or(0);
            // Incoming storage already exists. Record its actual peak even on
            // immediate cancellation/denial; this performs no new allocation.
            allocations.claim_bytes(input_bytes, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(fork_selection_allocation)?;
            cancellation.check()?;
            work.verify_ref(&budget)?;
            if !self.same_deployment(&source.workspace.volume.fs)
                || !self.same_deployment(&initial.workspace.volume.fs) {
                return Err(WorkspaceError::ForeignGeneration);
            }
            // Transfer the owned input to the materializer. It adopts the same
            // backing; only still-parent-owned storage offsets nested work.
            allocations.release(input_bytes).map_err(fork_selection_allocation)?;
            let live = allocations.live_bytes();
            let (expected, source_root, spent) = self.materialize_forked_generation_root(
                source, initial.workspace.id(), source.workspace.volume.config, paths,
                workspace_remaining(work, budget, live)?, cancellation,
            ).await.map_err(|failure| fork_selection_failure(
                failure, &mut work, live, |error| error,
            ))?;
            fork_selection_merge_work(&mut work, spent, live, budget)?;
            let matches = expected.generation_root.digest == initial.id.digest();
            drop(expected);
            drop(source_root);
            Ok(matches)
        }.await;
        allocations.release(allocations.live_bytes()).map_err(fork_selection_allocation)
            .map_err(|error| OperationFailure::new(error, work))?;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn select_fork_paths(
        &self, source: &crate::Generation<A, O>, root: &GenerationRoot, paths: Vec<String>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<(ObjectId, WorkCounters), WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.select_fork_paths_with_accounting(source, root, paths, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|root| (root, work)).map_err(|error| OperationFailure::new(error, work))
    }

    /// Ordinary and original-native selection execute this same immutable plan.
    pub(super) async fn select_fork_paths_with_accounting(
        &self, source: &crate::Generation<A, O>, root: &GenerationRoot, paths: Vec<String>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        let baseline = fork_selection_standalone_live(accounting);
        let result = async {
            // The transferred input already exists, including spare capacities.
            // Its actual claim must survive a late budget/cancellation refusal.
            let input_bytes = workspace_owned_strings_bytes(&paths)?;
            let (allocation, checked) = accounting.record_actual_owned(input_bytes)?;
            let paths = Owned::new(paths, allocation, input_bytes);
            checked?;
            cancellation.check()?;
            accounting.admit(WorkCounters::UNCHARGED)?;
            let config = source.workspace.volume.config;
            let maximum = config.limits.maximum_mutations_per_batch;
            if paths.value.len() > maximum as usize { return Err(WorkspaceError::JoinLimit); }
            let mut whole_root = false;
            for path in &paths.value {
                fork_selection_items(accounting, cancellation)?;
                whole_root |= path == "/";
            }
            if whole_root {
                for path in &paths.value {
                    NamespacePath::validate_in_profile_with_accounting(
                        path, config.profile, config.limits, accounting, cancellation,
                    )?;
                }
                paths.retire(accounting)?;
                return Ok(root.file_table);
            }
            let mut selected = Owned::<Vec<Owned<NamespacePath>>>::allocate(0, accounting)?;
            for (index, path) in paths.value.iter().enumerate() {
                let mut duplicate = false;
                for prior in paths.value.iter().take(index) {
                    fork_selection_items(accounting, cancellation)?;
                    if prior == path { duplicate = true; break; }
                }
                if duplicate { continue; }
                let (path, allocation) = customer_path_with_accounting(path, config, accounting, cancellation)?;
                let bytes = path.retained_bytes();
                let path = Owned::new(path, allocation, bytes);
                fork_selection_grow(&mut selected, maximum as usize, accounting, cancellation)?;
                fork_file_table_copy::<Owned<NamespacePath>>(1, accounting, cancellation)?;
                selected.value.push(path);
            }
            paths.retire(accounting)?;
            if maximum == 0 { return Err(WorkspaceError::JoinLimit); }
            let mut directories = Owned::<Vec<ForkDirectorySelection>>::allocate(1, accounting)?;
            fork_file_table_copy::<ForkDirectorySelection>(1, accounting, cancellation)?;
            directories.value.push(ForkDirectorySelection {
                path: None, depth: 0, children: Owned::<Vec<usize>>::allocate(0, accounting)?,
            });
            // Sorted borrowed prefix keys retain canonical path indices only.
            for (index, path) in selected.value.iter().enumerate() {
                for depth in 0..path.value.depth() {
                    cancellation.check()?;
                    let prefix = path.value.components().get(..depth).ok_or(WorkspaceError::JoinLimit)?;
                    let mut inherited = false;
                    for included in &selected.value {
                        if included.value.depth() <= depth && fork_selection_compare_names(
                            prefix.get(..included.value.depth()).ok_or(WorkspaceError::JoinLimit)?,
                            included.value.components(), accounting, cancellation,
                        )?.is_eq() { inherited = true; break; }
                    }
                    if inherited { continue; }
                    let mut lower = 0;
                    let mut upper = directories.value.len();
                    let mut existing = false;
                    while lower < upper {
                        let middle = lower + (upper - lower) / 2;
                        let plan = directories.value.get(middle).ok_or(WorkspaceError::JoinLimit)?;
                        match fork_selection_compare_names(fork_directory_components(plan, &selected.value)?, prefix, accounting, cancellation)? {
                            std::cmp::Ordering::Less => lower = middle + 1,
                            std::cmp::Ordering::Greater => upper = middle,
                            std::cmp::Ordering::Equal => { lower = middle; existing = true; break; }
                        }
                    }
                    if !existing {
                        if directories.value.len() >= maximum as usize { return Err(WorkspaceError::JoinLimit); }
                        let moved = directories.value.len() - lower + 1;
                        fork_selection_grow(&mut directories, maximum as usize, accounting, cancellation)?;
                        fork_file_table_copy::<ForkDirectorySelection>(moved, accounting, cancellation)?;
                        directories.value.insert(lower, ForkDirectorySelection {
                            path: Some(index), depth, children: Owned::<Vec<usize>>::allocate(0, accounting)?,
                        });
                    }
                    let plan = directories.value.get_mut(lower).ok_or(WorkspaceError::JoinLimit)?;
                    let name = path.value.components().get(depth).ok_or(WorkspaceError::JoinLimit)?;
                    let mut name_lower = 0;
                    let mut name_upper = plan.children.value.len();
                    let mut present = false;
                    while name_lower < name_upper {
                        let middle = name_lower + (name_upper - name_lower) / 2;
                        let child = plan.children.value.get(middle).and_then(|index| selected.value.get(*index))
                            .and_then(|path| path.value.components().get(depth)).ok_or(WorkspaceError::JoinLimit)?;
                        match fork_selection_compare_names(std::slice::from_ref(child), std::slice::from_ref(name), accounting, cancellation)? {
                            std::cmp::Ordering::Less => name_lower = middle + 1,
                            std::cmp::Ordering::Greater => name_upper = middle,
                            std::cmp::Ordering::Equal => { present = true; break; }
                        }
                    }
                    if !present {
                        let moved = plan.children.value.len() - name_lower + 1;
                        fork_selection_grow(&mut plan.children, maximum as usize, accounting, cancellation)?;
                        fork_file_table_copy::<usize>(moved, accounting, cancellation)?;
                        plan.children.value.insert(name_lower, index);
                    }
                }
            }
            let (mut checkout, mut checkout_bytes) = fork_selection_checkout(source, accounting, cancellation).await?;
            let mut overrides = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
            // Drain keeps the physical directory backing and its guard together.
            for plan in directories.value.drain(..) {
                fork_selection_items(accounting, cancellation)?;
                let (directory, allocation) = match plan.path {
                    Some(index) => selected.value.get(index).ok_or(WorkspaceError::JoinLimit)?.value
                        .prefix_with_accounting(plan.depth, config.limits, accounting, cancellation)?,
                    None => (NamespacePath::new(Vec::new(), config.limits).map_err(crate::workspace::namespace_path_error)?,
                        accounting.claim_owned(0, 0)?),
                };
                let directory_bytes = directory.retained_bytes();
                let directory = Owned::new(directory, allocation, directory_bytes);
                let (lookup, allocation) = checkout.lookup_no_follow_with_accounting(&directory.value, accounting, cancellation).await?;
                fork_selection_reconcile_checkout(&checkout, &mut checkout_bytes, accounting)?;
                let original = lookup.record.ok_or(WorkspaceError::NotFound)?;
                drop(lookup);
                drop(allocation);
                if original.kind != FileKind::Directory { return Err(WorkspaceError::NotDirectory); }
                let mut entries = Owned::<Vec<TreeMutation>>::allocate(plan.children.value.len(), accounting)?;
                for index in &plan.children.value {
                    fork_selection_items(accounting, cancellation)?;
                    let selected_path = &selected.value.get(*index).ok_or(WorkspaceError::JoinLimit)?.value;
                    let name = selected_path.components().get(plan.depth).ok_or(WorkspaceError::JoinLimit)?;
                    let (path, allocation) = selected_path.prefix_with_accounting(
                        plan.depth.checked_add(1).ok_or(WorkError::Overflow)?, config.limits, accounting, cancellation,
                    )?;
                    let bytes = path.retained_bytes();
                    let path = Owned::new(path, allocation, bytes);
                    let (lookup, allocation) = checkout.lookup_no_follow_with_accounting(&path.value, accounting, cancellation).await?;
                    fork_selection_reconcile_checkout(&checkout, &mut checkout_bytes, accounting)?;
                    let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
                    drop(lookup);
                    drop(allocation);
                    path.retire(accounting)?;
                    fork_file_table_copy::<TreeMutation>(1, accounting, cancellation)?;
                    let (name, allocation) = name.clone_with_accounting(accounting, cancellation)?;
                    let name_bytes = name.retained_bytes() as u64;
                    let mut name = Owned::new(name, allocation, name_bytes);
                    let retained = entries.bytes.checked_add(name_bytes).ok_or(WorkError::Overflow)?;
                    entries.allocation.absorb(&mut name.allocation)?;
                    entries.bytes = retained;
                    entries.value.push(TreeMutation::Insert(TreeEntry { name: name.value, file_id: record.file_id, kind: record.kind }));
                }
                directory.retire(accounting)?;
                plan.children.retire(accounting)?;
                let encoded = encode_tree_page_with_accounting(
                    &TreePage::Leaf(Vec::new()), config.limits.maximum_directory_page_entries, accounting,
                )?;
                let encoded_bytes = workspace_vec_bytes(&encoded.0);
                let empty = put_encoded_with_accounting(&self.inner.objects, ObjectKind::TreePage,
                    Owned::new(encoded.0, encoded.1, encoded_bytes), accounting, cancellation).await?;
                let tree_root = if entries.value.is_empty() { empty } else {
                    apply_tree_mutations_with_accounting(&self.inner.objects, empty, &entries.value, maximum,
                        DecodeLimits::for_volume(config), accounting, cancellation).await?.root
                };
                entries.retire(accounting)?;
                let record = FileRecord { payload: FilePayload::Directory { entries: tree_root }, ..original };
                let mut existing = None;
                for (index, prior) in overrides.value.iter().enumerate() {
                    fork_selection_items(accounting, cancellation)?;
                    if prior.file_id == record.file_id { existing = Some(index); break; }
                }
                if let Some(index) = existing {
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    *overrides.value.get_mut(index).ok_or(WorkspaceError::JoinLimit)? = record;
                } else {
                    fork_selection_grow(&mut overrides, maximum as usize, accounting, cancellation)?;
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    overrides.value.push(record);
                }
            }
            directories.retire(accounting)?;
            for path in selected.value.drain(..) { path.retire(accounting)?; }
            selected.retire(accounting)?;
            drop(checkout);
            if let Some(bytes) = checkout_bytes {
                accounting.release_owned(&mut KernelOwnedAllocation::Standalone, bytes)?;
            }
            self.select_fork_file_table_owned(root, overrides, DecodeLimits::for_volume(config), maximum, accounting, cancellation).await
        }.await;
        fork_selection_cleanup_standalone(accounting, baseline)?;
        result
    }

    pub(super) async fn select_fork_file_table_with_accounting(
        &self, root: &GenerationRoot, overrides: Vec<FileRecord>, limits: DecodeLimits, maximum: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        let baseline = fork_selection_standalone_live(accounting);
        let result = async {
            let bytes = workspace_vec_bytes(&overrides);
            let (allocation, checked) = accounting.record_actual_owned(bytes)?;
            let overrides = Owned::new(overrides, allocation, bytes);
            checked?;
            self.select_fork_file_table_owned(root, overrides, limits, maximum, accounting, cancellation).await
        }.await;
        fork_selection_cleanup_standalone(accounting, baseline)?;
        result
    }

    async fn select_fork_file_table_owned(
        &self, root: &GenerationRoot, overrides: Owned<Vec<FileRecord>>, limits: DecodeLimits, maximum: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        cancellation.check()?;
        if maximum == 0 || overrides.value.len() > maximum as usize { return Err(WorkspaceError::JoinLimit); }
        let mut pending = Owned::<Vec<FileId>>::allocate(1, accounting)?;
        fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
        pending.value.push(root.root_file_id);
        let mut links = Owned::<Vec<(FileId, u64)>>::allocate(1, accounting)?;
        fork_file_table_copy::<(FileId, u64)>(1, accounting, cancellation)?;
        links.value.push((root.root_file_id, 1));
        let mut records = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
        while !pending.value.is_empty() {
            fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
            let id = pending.value.pop().ok_or(WorkspaceError::JoinLimit)?;
            let position = match fork_file_table_search(&records.value, id, |record| record.file_id, accounting, cancellation)? {
                Ok(_) => continue, Err(position) => position,
            };
            if records.value.len() >= maximum as usize { return Err(WorkspaceError::JoinLimit); }
            let mut overridden = None;
            for record in &overrides.value {
                fork_selection_items(accounting, cancellation)?;
                if record.file_id == id {
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    overridden = Some(*record); break;
                }
            }
            let record = if let Some(record) = overridden { record } else {
                let (record, allocation) = lookup_file_record_async_with_accounting(
                    &self.inner.objects, root.file_table, id, limits, accounting, cancellation, None,
                ).await?;
                drop(allocation);
                record.ok_or(WorkspaceError::NotFound)?
            };
            fork_selection_grow(&mut records, maximum as usize, accounting, cancellation)?;
            fork_file_table_copy::<FileRecord>(records.value.len() - position + 1, accounting, cancellation)?;
            records.value.insert(position, record);
            if let FilePayload::Directory { entries } = record.payload {
                let mut cursor: Option<Owned<LogicalName>> = None;
                loop {
                    let (page, allocation) = list_tree_entries_async_with_accounting(
                        &self.inner.objects, entries, cursor.as_ref().map(|cursor| &cursor.value),
                        limits.maximum_page_items, limits, accounting, cancellation, None,
                    ).await?;
                    let mut page = Owned::new(page, allocation, 0);
                    page.bytes = page.value.entries.iter().try_fold(workspace_vec_bytes(&page.value.entries), |bytes, entry| {
                        bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(WorkError::Overflow)
                    })?;
                    for entry in &page.value.entries {
                        fork_selection_items(accounting, cancellation)?;
                        if pending.value.len() >= maximum as usize { return Err(WorkspaceError::JoinLimit); }
                        let child = entry.file_id;
                        match fork_file_table_search(&links.value, child, |binding| binding.0, accounting, cancellation)? {
                            Ok(position) => {
                                links.value[position].1 = links.value[position].1.checked_add(1).ok_or(WorkspaceError::JoinLimit)?;
                            }
                            Err(position) => {
                                if links.value.len() >= maximum as usize { return Err(WorkspaceError::JoinLimit); }
                                fork_selection_grow(&mut links, maximum as usize, accounting, cancellation)?;
                                fork_file_table_copy::<(FileId, u64)>(links.value.len() - position + 1, accounting, cancellation)?;
                                links.value.insert(position, (child, 1));
                            }
                        }
                        fork_selection_grow(&mut pending, maximum as usize, accounting, cancellation)?;
                        fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
                        pending.value.push(child);
                    }
                    let has_more = page.value.has_more;
                    let next_cursor = if has_more {
                        fork_file_table_copy::<TreeEntry>(1, accounting, cancellation)?;
                        let bytes = page.value.entries.last().ok_or(WorkspaceError::JoinLimit)?.name.retained_bytes() as u64;
                        let remaining = page.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
                        let allocation = match &mut page.allocation {
                            KernelOwnedAllocation::Standalone => KernelOwnedAllocation::Standalone,
                            #[cfg(not(target_arch = "wasm32"))]
                            KernelOwnedAllocation::NativeOriginal(guard) => KernelOwnedAllocation::NativeOriginal(guard.split_off(bytes)?),
                        };
                        let name = page.value.entries.pop().ok_or(WorkspaceError::JoinLimit)?.name;
                        page.bytes = remaining;
                        Some(Owned::new(name, allocation, bytes))
                    } else { None };
                    if let Some(cursor) = cursor.take() { cursor.retire(accounting)?; }
                    cursor = next_cursor;
                    page.retire(accounting)?;
                    if !has_more { break; }
                }
            }
        }
        pending.retire(accounting)?;
        overrides.retire(accounting)?;
        let mut mutations = Owned::<Vec<FileTableMutation>>::allocate(records.value.len(), accounting)?;
        for record in &records.value {
            fork_selection_items(accounting, cancellation)?;
            let position = fork_file_table_search(&links.value, record.file_id, |binding| binding.0, accounting, cancellation)?
                .map_err(|_| WorkspaceError::JoinLimit)?;
            fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
            let mut record = *record;
            record.link_count = links.value[position].1;
            fork_file_table_copy::<FileTableMutation>(1, accounting, cancellation)?;
            mutations.value.push(FileTableMutation::Insert(record));
        }
        records.retire(accounting)?;
        links.retire(accounting)?;
        let encoded = encode_file_table_page_with_accounting(&FileTablePage::Leaf(Vec::new()), limits.maximum_page_items, accounting)?;
        let encoded_bytes = workspace_vec_bytes(&encoded.0);
        let empty = put_encoded_with_accounting(&self.inner.objects, ObjectKind::FileTablePage,
            Owned::new(encoded.0, encoded.1, encoded_bytes), accounting, cancellation).await?;
        let table = apply_file_table_mutations_with_accounting(&self.inner.objects, empty, &mutations.value,
            maximum, limits, accounting, cancellation).await?;
        mutations.retire(accounting)?;
        Ok(table.root)
    }
}

fn fork_selection_items(accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
    cancellation.check()?;
    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    Ok(())
}

fn fork_file_table_copy<T>(elements: usize, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
    cancellation.check()?;
    let bytes = elements.checked_mul(std::mem::size_of::<T>()).ok_or(WorkError::Overflow)?;
    accounting.charge(WorkCounters { bytes_copied: u64::try_from(bytes).map_err(|_| WorkError::Overflow)?, ..WorkCounters::UNCHARGED })?;
    Ok(())
}

fn fork_selection_grow<T>(owner: &mut Owned<Vec<T>>, maximum: usize, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
    if owner.value.len() == owner.value.capacity() {
        fork_file_table_copy::<T>(owner.value.len(), accounting, cancellation)?;
    }
    owner.ensure_for_push(maximum, accounting)?;
    Ok(())
}

fn fork_file_table_search<T>(values: &[T], id: FileId, mut key: impl FnMut(&T) -> FileId,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Result<usize, usize>, WorkspaceError> {
    let mut start = 0;
    let mut end = values.len();
    while start < end {
        fork_selection_items(accounting, cancellation)?;
        let middle = start + (end - start) / 2;
        match key(&values[middle]).cmp(&id) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            std::cmp::Ordering::Greater => end = middle,
        }
    }
    Ok(Err(start))
}

fn fork_selection_standalone_live(accounting: &mut KernelAccounting<'_>) -> Option<u64> {
    accounting.standalone_parts().map(|(ledger, _, _, _)| ledger.live_bytes())
}

// Only standalone has aggregate bookkeeping; native guards retire themselves
// after their actual data. The completed future has destroyed every local here.
fn fork_selection_cleanup_standalone(accounting: &mut KernelAccounting<'_>, baseline: Option<u64>) -> Result<(), WorkspaceError> {
    if let Some(baseline) = baseline {
        let (ledger, _, _, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
        ledger.release(ledger.live_bytes().checked_sub(baseline).ok_or(WorkError::Overflow)?)
            .map_err(fork_selection_allocation)?;
    }
    Ok(())
}

async fn fork_selection_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    source: &crate::Generation<A, O>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(Checkout<A, O>, Option<u64>), WorkspaceError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return source.workspace.volume.checkout_with_native_work(
            GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(), original, cancellation,
        ).await.map(|checkout| (checkout, None)).map_err(WorkspaceError::from);
    }
    let receipt = source.workspace.volume.checkout(
        GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(), accounting.remaining()?, cancellation,
    ).await;
    let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    let (ledger, work, budget, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
    *work = work.with_backend(spent, ledger.live_bytes())?;
    work.verify_ref(&budget)?;
    let checkout = receipt.map_err(|failure| WorkspaceError::from(failure.error))?.value;
    let bytes = checkout.logical_owned_bytes();
    let (allocation, checked) = accounting.record_actual_owned(bytes)?;
    drop(allocation);
    checked?;
    Ok((checkout, Some(bytes)))
}

fn fork_selection_reconcile_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>, retained: &mut Option<u64>, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    if let Some(previous) = retained {
        let actual = checkout.logical_owned_bytes();
        let checked = if actual >= *previous {
            let (allocation, checked) = accounting.record_actual_owned(actual - *previous)?;
            drop(allocation);
            checked
        } else {
            accounting.release_owned(&mut KernelOwnedAllocation::Standalone, *previous - actual)
        };
        *previous = actual;
        checked?;
    }
    Ok(())
}
