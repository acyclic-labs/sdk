//! Immutable namespace selection using the caller's original accounting owner.

use super::fork_selection_work::*;
use super::*;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::{
    FileTableMutation, FileTablePage, Owned, TreeEntry, TreeMutation,
    apply_file_table_mutations_with_accounting, apply_tree_mutations_with_accounting,
    encode_file_table_page, encode_tree_page, list_tree_entries_async_with_accounting,
    lookup_file_record_async_with_accounting,
};
use crate::workspace::{
    WorkspaceError, customer_path_with_accounting, workspace_owned_strings_bytes,
    workspace_vec_bytes,
};

struct ForkDirectorySelection {
    path: Option<usize>,
    depth: usize,
    children: Owned<Vec<usize>>,
}

fn fork_selection_compare_names(
    left: &[LogicalName],
    right: &[LogicalName],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<std::cmp::Ordering, WorkspaceError> {
    fork_selection_items(accounting, cancellation)?;
    for (left, right) in left.iter().zip(right) {
        fork_selection_items(accounting, cancellation)?;
        let order = left.cmp(right);
        if !order.is_eq() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn fork_selection_search(
    length: usize,
    mut compare: impl FnMut(usize) -> Result<std::cmp::Ordering, WorkspaceError>,
) -> Result<(usize, bool), WorkspaceError> {
    let (mut lower, mut upper) = (0, length);
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        match compare(middle)? {
            std::cmp::Ordering::Less => lower = middle + 1,
            std::cmp::Ordering::Greater => upper = middle,
            std::cmp::Ordering::Equal => return Ok((middle, true)),
        }
    }
    Ok((lower, false))
}

fn fork_directory_components<'a>(
    directory: &ForkDirectorySelection,
    selected: &'a [Owned<NamespacePath>],
) -> Result<&'a [LogicalName], WorkspaceError> {
    match directory.path {
        None => Ok(&[]),
        Some(index) => selected
            .get(index)
            .and_then(|path| path.value.components().get(..directory.depth))
            .ok_or(WorkspaceError::JoinLimit),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn verify_fork_selection<W: FsWork>(
        &self,
        source: &crate::Generation<A, O>,
        initial: &crate::Generation<A, O>,
        paths: Option<Vec<String>>,
        funding: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        let mut accounting = funding.accounting(cancellation);
        let baseline = readonly_standalone_live(&mut accounting);
        let result = async {
            let input_bytes = paths
                .as_ref()
                .map(workspace_owned_strings_bytes)
                .transpose()?
                .unwrap_or(0);
            let (allocation, admitted) = accounting.record_actual_owned(input_bytes)?;
            let mut paths = (paths, allocation);
            match &accounting {
                KernelAccounting::Standalone { .. }
                | KernelAccounting::BorrowedStandalone { .. } => cancellation.check()?,
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } => {}
            }
            admitted?;
            accounting.check()?;
            if !self.same_deployment(&source.workspace.volume.fs)
                || !self.same_deployment(&initial.workspace.volume.fs)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let selected = paths.0.take();
            accounting.release_owned(&mut paths.1, input_bytes)?;
            let (expected, _) = self
                .materialize_forked_generation_root(
                    source,
                    initial.workspace.id(),
                    source.workspace.volume.config,
                    selected,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(expected.value.generation_root.digest == initial.id.digest())
        }
        .await;
        let result = readonly_cleanup_standalone(&mut accounting, baseline, 0)
            .map_err(WorkspaceError::from)
            .and(result);
        funding.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Ordinary and original-native selection execute this same immutable plan.
    pub(super) async fn select_fork_paths_with_accounting(
        &self,
        source: &crate::Generation<A, O>,
        root: &GenerationRoot,
        paths: Vec<String>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        let baseline = readonly_standalone_live(accounting);
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
            if paths.value.len() > maximum as usize {
                return Err(WorkspaceError::JoinLimit);
            }
            let mut whole_root = false;
            for path in &paths.value {
                fork_selection_items(accounting, cancellation)?;
                whole_root |= path == "/";
            }
            if whole_root {
                for path in &paths.value {
                    NamespacePath::validate_in_profile_with_accounting(
                        path,
                        config.profile,
                        config.limits,
                        accounting,
                        cancellation,
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
                    if prior == path {
                        duplicate = true;
                        break;
                    }
                }
                if duplicate {
                    continue;
                }
                let (path, allocation) =
                    customer_path_with_accounting(path, config, accounting, cancellation)?;
                let bytes = path.retained_bytes();
                let path = Owned::new(path, allocation, bytes);
                fork_selection_grow(&mut selected, maximum as usize, accounting, cancellation)?;
                fork_file_table_copy::<Owned<NamespacePath>>(1, accounting, cancellation)?;
                selected.value.push(path);
            }
            paths.retire(accounting)?;
            if maximum == 0 {
                return Err(WorkspaceError::JoinLimit);
            }
            let mut directories = Owned::<Vec<ForkDirectorySelection>>::allocate(1, accounting)?;
            fork_file_table_copy::<ForkDirectorySelection>(1, accounting, cancellation)?;
            directories.value.push(ForkDirectorySelection {
                path: None,
                depth: 0,
                children: Owned::<Vec<usize>>::allocate(0, accounting)?,
            });
            // Sorted borrowed prefix keys retain canonical path indices only.
            for (index, path) in selected.value.iter().enumerate() {
                for depth in 0..path.value.depth() {
                    cancellation.check()?;
                    let prefix = path
                        .value
                        .components()
                        .get(..depth)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    let mut inherited = false;
                    for included in &selected.value {
                        if included.value.depth() <= depth
                            && fork_selection_compare_names(
                                prefix
                                    .get(..included.value.depth())
                                    .ok_or(WorkspaceError::JoinLimit)?,
                                included.value.components(),
                                accounting,
                                cancellation,
                            )?
                            .is_eq()
                        {
                            inherited = true;
                            break;
                        }
                    }
                    if inherited {
                        continue;
                    }
                    let (lower, existing) =
                        fork_selection_search(directories.value.len(), |middle| {
                            let plan = directories
                                .value
                                .get(middle)
                                .ok_or(WorkspaceError::JoinLimit)?;
                            fork_selection_compare_names(
                                fork_directory_components(plan, &selected.value)?,
                                prefix,
                                accounting,
                                cancellation,
                            )
                        })?;
                    if !existing {
                        if directories.value.len() >= maximum as usize {
                            return Err(WorkspaceError::JoinLimit);
                        }
                        let moved = directories.value.len() - lower + 1;
                        fork_selection_grow(
                            &mut directories,
                            maximum as usize,
                            accounting,
                            cancellation,
                        )?;
                        fork_file_table_copy::<ForkDirectorySelection>(
                            moved,
                            accounting,
                            cancellation,
                        )?;
                        directories.value.insert(
                            lower,
                            ForkDirectorySelection {
                                path: Some(index),
                                depth,
                                children: Owned::<Vec<usize>>::allocate(0, accounting)?,
                            },
                        );
                    }
                    let plan = directories
                        .value
                        .get_mut(lower)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    let name = path
                        .value
                        .components()
                        .get(depth)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    let (name_lower, present) =
                        fork_selection_search(plan.children.value.len(), |middle| {
                            let child = plan
                                .children
                                .value
                                .get(middle)
                                .and_then(|index| selected.value.get(*index))
                                .and_then(|path| path.value.components().get(depth))
                                .ok_or(WorkspaceError::JoinLimit)?;
                            fork_selection_compare_names(
                                std::slice::from_ref(child),
                                std::slice::from_ref(name),
                                accounting,
                                cancellation,
                            )
                        })?;
                    if !present {
                        let moved = plan.children.value.len() - name_lower + 1;
                        fork_selection_grow(
                            &mut plan.children,
                            maximum as usize,
                            accounting,
                            cancellation,
                        )?;
                        fork_file_table_copy::<usize>(moved, accounting, cancellation)?;
                        plan.children.value.insert(name_lower, index);
                    }
                }
            }
            let (mut checkout, mut checkout_bytes) =
                fork_selection_checkout(source, accounting, cancellation).await?;
            let mut overrides = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
            // Drain keeps the physical directory backing and its guard together.
            for plan in directories.value.drain(..) {
                fork_selection_items(accounting, cancellation)?;
                let (directory, allocation) = match plan.path {
                    Some(index) => selected
                        .value
                        .get(index)
                        .ok_or(WorkspaceError::JoinLimit)?
                        .value
                        .prefix_with_accounting(
                            plan.depth,
                            config.limits,
                            accounting,
                            cancellation,
                        )?,
                    None => (
                        NamespacePath::new(Vec::new(), config.limits)
                            .map_err(WorkspaceError::from)?,
                        accounting.claim_owned(0, 0)?,
                    ),
                };
                let directory_bytes = directory.retained_bytes();
                let directory = Owned::new(directory, allocation, directory_bytes);
                let (lookup, allocation) = checkout
                    .lookup_no_follow_with_accounting(
                        &directory.value,
                        accounting,
                        cancellation,
                        true,
                    )
                    .await?;
                fork_selection_reconcile_checkout(&checkout, &mut checkout_bytes, accounting)?;
                let original = lookup.record.ok_or(WorkspaceError::NotFound)?;
                drop(allocation);
                if original.kind != FileKind::Directory {
                    return Err(WorkspaceError::NotDirectory);
                }
                let mut entries =
                    Owned::<Vec<TreeMutation>>::allocate(plan.children.value.len(), accounting)?;
                for index in &plan.children.value {
                    fork_selection_items(accounting, cancellation)?;
                    let selected_path = &selected
                        .value
                        .get(*index)
                        .ok_or(WorkspaceError::JoinLimit)?
                        .value;
                    let name = selected_path
                        .components()
                        .get(plan.depth)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    let (path, allocation) = selected_path.prefix_with_accounting(
                        plan.depth.checked_add(1).ok_or(WorkError::Overflow)?,
                        config.limits,
                        accounting,
                        cancellation,
                    )?;
                    let bytes = path.retained_bytes();
                    let path = Owned::new(path, allocation, bytes);
                    let (lookup, allocation) = checkout
                        .lookup_no_follow_with_accounting(
                            &path.value,
                            accounting,
                            cancellation,
                            true,
                        )
                        .await?;
                    fork_selection_reconcile_checkout(&checkout, &mut checkout_bytes, accounting)?;
                    let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
                    drop(allocation);
                    path.retire(accounting)?;
                    fork_file_table_copy::<TreeMutation>(1, accounting, cancellation)?;
                    let (name, allocation) =
                        name.clone_with_accounting(accounting, cancellation)?;
                    let name_bytes = name.retained_bytes() as u64;
                    let mut name = Owned::new(name, allocation, name_bytes);
                    let retained = entries
                        .bytes
                        .checked_add(name_bytes)
                        .ok_or(WorkError::Overflow)?;
                    entries.allocation.absorb(&mut name.allocation)?;
                    entries.bytes = retained;
                    entries.value.push(TreeMutation::Insert(TreeEntry {
                        name: name.value,
                        file_id: record.file_id,
                        kind: record.kind,
                    }));
                }
                directory.retire(accounting)?;
                plan.children.retire(accounting)?;
                let encoded =
                    crate::kernel::codec::encode_owned(accounting, |observe, _reserve| {
                        cancellation.check()?;
                        encode_tree_page(
                            &TreePage::Leaf(Vec::new()),
                            config.limits.maximum_directory_page_entries,
                            observe,
                            crate::FsError::EncodedAllocationFailed,
                        )
                    })?;
                let empty = self
                    .put_encoded_with_accounting(
                        ObjectKind::TreePage,
                        encoded,
                        accounting,
                        cancellation,
                    )
                    .await?;
                let tree_root = if entries.value.is_empty() {
                    empty
                } else {
                    apply_tree_mutations_with_accounting(
                        &self.inner.objects,
                        empty,
                        &entries.value,
                        maximum,
                        DecodeLimits::for_volume(config),
                        accounting,
                        cancellation,
                    )
                    .await?
                    .value
                };
                entries.retire(accounting)?;
                let record = FileRecord {
                    payload: FilePayload::Directory { entries: tree_root },
                    ..original
                };
                let mut existing = None;
                for (index, prior) in overrides.value.iter().enumerate() {
                    fork_selection_items(accounting, cancellation)?;
                    if prior.file_id == record.file_id {
                        existing = Some(index);
                        break;
                    }
                }
                if let Some(index) = existing {
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    *overrides
                        .value
                        .get_mut(index)
                        .ok_or(WorkspaceError::JoinLimit)? = record;
                } else {
                    fork_selection_grow(
                        &mut overrides,
                        maximum as usize,
                        accounting,
                        cancellation,
                    )?;
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    overrides.value.push(record);
                }
            }
            directories.retire(accounting)?;
            for path in selected.value.drain(..) {
                path.retire(accounting)?;
            }
            selected.retire(accounting)?;
            drop(checkout);
            if let Some(bytes) = checkout_bytes {
                accounting.release_owned(&mut KernelOwnedAllocation::Standalone, bytes)?;
            }
            self.select_fork_file_table_owned(
                root,
                overrides,
                DecodeLimits::for_volume(config),
                maximum,
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        readonly_cleanup_standalone(accounting, baseline, 0)?;
        result
    }

    async fn select_fork_file_table_owned(
        &self,
        root: &GenerationRoot,
        overrides: Owned<Vec<FileRecord>>,
        limits: DecodeLimits,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        cancellation.check()?;
        if maximum == 0 || overrides.value.len() > maximum as usize {
            return Err(WorkspaceError::JoinLimit);
        }
        let mut pending = Owned::<Vec<FileId>>::allocate(1, accounting)?;
        fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
        pending.value.push(root.root_file_id);
        let mut records = Owned::<Vec<(FileId, u64, Option<FileRecord>)>>::allocate(1, accounting)?;
        fork_file_table_copy::<(FileId, u64, Option<FileRecord>)>(1, accounting, cancellation)?;
        records.value.push((root.root_file_id, 1, None));
        while !pending.value.is_empty() {
            fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
            let id = pending.value.pop().ok_or(WorkspaceError::JoinLimit)?;
            let position = crate::kernel::binary_search_with_accounting(
                &records.value,
                accounting,
                |entry| entry.0.cmp(&id),
            )?
            .map_err(|_| WorkspaceError::JoinLimit)?;
            if records.value[position].2.is_some() {
                continue;
            }
            let mut overridden = None;
            for record in &overrides.value {
                fork_selection_items(accounting, cancellation)?;
                if record.file_id == id {
                    fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
                    overridden = Some(*record);
                    break;
                }
            }
            let record = if let Some(record) = overridden {
                record
            } else {
                let (record, allocation) = lookup_file_record_async_with_accounting(
                    &self.inner.objects,
                    root.file_table,
                    id,
                    limits,
                    accounting,
                    cancellation,
                    None,
                )
                .await?;
                drop(allocation);
                record.ok_or(WorkspaceError::NotFound)?
            };
            fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
            records.value[position].2 = Some(record);
            if let FilePayload::Directory { entries } = record.payload {
                let mut cursor: Option<Owned<LogicalName>> = None;
                loop {
                    let (page, allocation) = list_tree_entries_async_with_accounting(
                        &self.inner.objects,
                        entries,
                        cursor.as_ref().map(|cursor| &cursor.value),
                        limits.maximum_page_items,
                        limits,
                        accounting,
                        cancellation.clone(),
                        None,
                    )
                    .await?;
                    let mut page = Owned::new(page, allocation, 0);
                    page.bytes = page.value.entries.iter().try_fold(
                        workspace_vec_bytes(&page.value.entries),
                        |bytes, entry| {
                            bytes
                                .checked_add(entry.name.retained_bytes() as u64)
                                .ok_or(WorkError::Overflow)
                        },
                    )?;
                    for entry in &page.value.entries {
                        fork_selection_items(accounting, cancellation)?;
                        if pending.value.len() >= maximum as usize {
                            return Err(WorkspaceError::JoinLimit);
                        }
                        let child = entry.file_id;
                        match crate::kernel::binary_search_with_accounting(
                            &records.value,
                            accounting,
                            |binding| binding.0.cmp(&child),
                        )? {
                            Ok(position) => {
                                records.value[position].1 = records.value[position]
                                    .1
                                    .checked_add(1)
                                    .ok_or(WorkspaceError::JoinLimit)?;
                            }
                            Err(position) => {
                                if records.value.len() >= maximum as usize {
                                    return Err(WorkspaceError::JoinLimit);
                                }
                                fork_selection_grow(
                                    &mut records,
                                    maximum as usize,
                                    accounting,
                                    cancellation,
                                )?;
                                fork_file_table_copy::<(FileId, u64, Option<FileRecord>)>(
                                    records.value.len() - position + 1,
                                    accounting,
                                    cancellation,
                                )?;
                                records.value.insert(position, (child, 1, None));
                            }
                        }
                        fork_selection_grow(
                            &mut pending,
                            maximum as usize,
                            accounting,
                            cancellation,
                        )?;
                        fork_file_table_copy::<FileId>(1, accounting, cancellation)?;
                        pending.value.push(child);
                    }
                    let has_more = page.value.has_more;
                    let next_cursor = if has_more {
                        fork_file_table_copy::<TreeEntry>(1, accounting, cancellation)?;
                        let bytes = page
                            .value
                            .entries
                            .last()
                            .ok_or(WorkspaceError::JoinLimit)?
                            .name
                            .retained_bytes() as u64;
                        let remaining = page.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
                        let allocation =
                            crate::kernel::split_allocation(&mut page.allocation, bytes)?;
                        let name = page
                            .value
                            .entries
                            .pop()
                            .ok_or(WorkspaceError::JoinLimit)?
                            .name;
                        page.bytes = remaining;
                        Some(Owned::new(name, allocation, bytes))
                    } else {
                        None
                    };
                    if let Some(cursor) = cursor.take() {
                        cursor.retire(accounting)?;
                    }
                    cursor = next_cursor;
                    page.retire(accounting)?;
                    if !has_more {
                        break;
                    }
                }
            }
        }
        pending.retire(accounting)?;
        overrides.retire(accounting)?;
        let mut mutations =
            Owned::<Vec<FileTableMutation>>::allocate(records.value.len(), accounting)?;
        for (_, count, record) in &records.value {
            fork_selection_items(accounting, cancellation)?;
            fork_file_table_copy::<FileRecord>(1, accounting, cancellation)?;
            let mut record = record.ok_or(WorkspaceError::NotFound)?;
            record.link_count = *count;
            fork_file_table_copy::<FileTableMutation>(1, accounting, cancellation)?;
            mutations.value.push(FileTableMutation::Insert(record));
        }
        records.retire(accounting)?;
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, reserve| {
            encode_file_table_page(
                &FileTablePage::Leaf(Vec::new()),
                limits.maximum_page_items,
                observe,
                reserve,
            )
        })?;
        let empty = self
            .put_encoded_with_accounting(
                ObjectKind::FileTablePage,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        let table = apply_file_table_mutations_with_accounting(
            &self.inner.objects,
            empty,
            &mutations.value,
            maximum,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        mutations.retire(accounting)?;
        Ok(table.value)
    }
}

// Only standalone has aggregate bookkeeping; native guards retire themselves
// after their actual data. The completed future has destroyed every local here.
