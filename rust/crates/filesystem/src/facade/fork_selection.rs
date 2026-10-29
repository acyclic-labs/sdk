//! Funded immutable namespace selection with ordered prefix traversal.

use super::*;
use crate::kernel::{
    FileTableMutation, FileTablePage, TreeEntry, TreeMutation,
    apply_file_table_mutations_with_accounting, apply_tree_mutations_with_accounting,
    encode_file_table_page, list_tree_entries_async_with_accounting,
    lookup_file_record_async_with_accounting,
};
use crate::workspace::{
    WorkspaceError, customer_path_with_accounting, workspace_owned_strings_bytes,
    workspace_vec_bytes,
};

/// A contiguous group of sorted paths sharing one directory prefix.
#[derive(Clone, Copy)]
struct SelectionDirectory {
    start: usize,
    end: usize,
    depth: usize,
}

fn sort_selection(
    paths: &mut [Owned<NamespacePath>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    crate::kernel::sort::heap_sort(
        paths,
        accounting,
        |left, right, accounting| {
            accounting.charge_items(1 + left.value.depth().min(right.value.depth()) as u64)?;
            Ok(left.value.components().cmp(right.value.components()))
        },
        |paths, left, right, accounting| {
            accounting.charge(WorkCounters {
                bytes_copied: (2 * size_of::<Owned<NamespacePath>>()) as u64,
                ..WorkCounters::UNCHARGED
            })?;
            paths.swap(left, right);
            Ok(())
        },
        |accounting| accounting.check(),
    )
}

pub(super) fn selection_push<T>(
    values: &mut Owned<Vec<T>>,
    value: T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copies = if values.value.len() == values.value.capacity() {
        values.value.len() + 1
    } else {
        1
    };
    accounting.charge(WorkCounters {
        bytes_copied: copies
            .checked_mul(size_of::<T>())
            .ok_or(WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    })?;
    values.ensure_for_push(usize::MAX, accounting)?;
    values.value.push(value);
    Ok(())
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
        let baseline = readonly_standalone_live(&accounting);
        let result = async {
            let input_bytes = paths
                .as_ref()
                .map(workspace_owned_strings_bytes)
                .transpose()?
                .unwrap_or(0);
            let (allocation, admitted) = accounting.record_actual_owned(input_bytes)?;
            let paths = Owned::new(paths, allocation, input_bytes);
            if readonly_standalone_live(&accounting).is_some() {
                cancellation.check()?;
            }
            admitted?;
            if !self.same_deployment(&source.workspace.volume.fs)
                || !self.same_deployment(&initial.workspace.volume.fs)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let (expected, _, _, _) = self
                .materialize_forked_generation_root(
                    source,
                    initial.workspace.id(),
                    paths,
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

    pub(super) async fn select_fork_paths(
        &self,
        source: &crate::Generation<A, O>,
        root: &GenerationRoot,
        paths: Owned<Vec<String>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        let baseline = readonly_standalone_live(accounting)
            .map(|live| live.checked_sub(paths.bytes).ok_or(WorkError::Overflow))
            .transpose()?;
        let result = async {
            let config = source.workspace.volume.config;
            // Validate the entire input before allocating parsed paths, including root selections.
            for path in &paths.value {
                NamespacePath::validate_in_profile_with_accounting(
                    path,
                    config.profile,
                    config.limits,
                    accounting,
                    cancellation,
                )?;
            }
            if paths.value.iter().any(|path| path == "/") {
                paths.retire(accounting)?;
                return Ok(root.file_table);
            }
            let limits = DecodeLimits::for_volume(config);
            let mut selected = Owned::<Vec<Owned<NamespacePath>>>::allocate(0, accounting)?;
            for path in &paths.value {
                let (path, allocation) =
                    customer_path_with_accounting(path, config, accounting, cancellation)?;
                let bytes = path.retained_bytes();
                selection_push(
                    &mut selected,
                    Owned::new(path, allocation, bytes),
                    accounting,
                )?;
            }
            paths.retire(accounting)?;
            sort_selection(&mut selected.value, accounting)?;
            let mut checkout = source
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(source.id),
                    CheckoutMode::read_only_pinned(),
                    accounting,
                    cancellation,
                )
                .await?;
            let empty = checkout
                .value
                .stage_empty_tree_with_accounting(accounting, cancellation)
                .await?;
            let mut pending = Owned::<Vec<SelectionDirectory>>::allocate(0, accounting)?;
            selection_push(
                &mut pending,
                SelectionDirectory {
                    start: 0,
                    end: selected.value.len(),
                    depth: 0,
                },
                accounting,
            )?;
            let mut overrides = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
            while let Some(directory) = pending.value.pop() {
                accounting.charge_items(1)?;
                let (directory_path, guard) = if directory.depth == 0 {
                    (
                        NamespacePath::new(Vec::new(), config.limits)?,
                        accounting.claim_owned(0, 0)?,
                    )
                } else {
                    selected
                        .value
                        .get(directory.start)
                        .ok_or(FsError::InvalidDiff)?
                        .value
                        .prefix_with_accounting(
                            directory.depth,
                            config.limits,
                            accounting,
                            cancellation,
                        )?
                };
                let bytes = directory_path.retained_bytes();
                let directory_path = Owned::new(directory_path, guard, bytes);
                let (lookup, guard) = checkout
                    .value
                    .lookup_no_follow_with_accounting(
                        &directory_path.value,
                        accounting,
                        cancellation,
                        false,
                    )
                    .await?;
                let lookup = Owned::new(lookup, guard, 0);
                let record = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
                lookup.retire(accounting)?;
                directory_path.retire(accounting)?;
                if record.kind != FileKind::Directory {
                    return Err(WorkspaceError::NotDirectory);
                }
                let mut mutations = Owned::<Vec<TreeMutation>>::allocate(0, accounting)?;
                let mut start = directory.start;
                while start < directory.end {
                    accounting.charge_items(1)?;
                    let path = &selected.value.get(start).ok_or(FsError::InvalidDiff)?.value;
                    let name = path
                        .components()
                        .get(directory.depth)
                        .ok_or(FsError::InvalidDiff)?;
                    let mut end = start + 1;
                    while end < directory.end {
                        accounting.charge_items(1)?;
                        let next = selected.value.get(end).ok_or(FsError::InvalidDiff)?;
                        if next.value.components().get(directory.depth) != Some(name) {
                            break;
                        }
                        end += 1;
                    }
                    let (child_path, guard) = path.prefix_with_accounting(
                        directory.depth + 1,
                        config.limits,
                        accounting,
                        cancellation,
                    )?;
                    let bytes = child_path.retained_bytes();
                    let child_path = Owned::new(child_path, guard, bytes);
                    let (lookup, guard) = checkout
                        .value
                        .lookup_no_follow_with_accounting(
                            &child_path.value,
                            accounting,
                            cancellation,
                            false,
                        )
                        .await?;
                    let lookup = Owned::new(lookup, guard, 0);
                    let child = lookup.value.record.ok_or(WorkspaceError::NotFound)?;
                    lookup.retire(accounting)?;
                    child_path.retire(accounting)?;
                    let explicit = path.depth() == directory.depth + 1;
                    if !explicit {
                        if child.kind != FileKind::Directory {
                            return Err(WorkspaceError::NotDirectory);
                        }
                        selection_push(
                            &mut pending,
                            SelectionDirectory {
                                start,
                                end,
                                depth: directory.depth + 1,
                            },
                            accounting,
                        )?;
                    }
                    let (name, mut guard) = name.clone_with_accounting(accounting, cancellation)?;
                    let bytes = name.retained_bytes() as u64;
                    mutations.allocation.absorb(&mut guard)?;
                    mutations.bytes = mutations
                        .bytes
                        .checked_add(bytes)
                        .ok_or(WorkError::Overflow)?;
                    selection_push(
                        &mut mutations,
                        TreeMutation::Insert(TreeEntry {
                            name,
                            file_id: child.file_id,
                            kind: child.kind,
                        }),
                        accounting,
                    )?;
                    start = end;
                }
                let entries = if mutations.value.is_empty() {
                    empty
                } else {
                    apply_tree_mutations_with_accounting(
                        &self.inner.objects,
                        empty,
                        &mutations.value,
                        limits,
                        accounting,
                        cancellation,
                    )
                    .await?
                    .value
                };
                mutations.retire(accounting)?;
                selection_push(
                    &mut overrides,
                    FileRecord {
                        payload: FilePayload::Directory { entries },
                        ..record
                    },
                    accounting,
                )?;
            }
            pending.retire(accounting)?;
            checkout.retire(accounting)?;
            for path in selected.value.drain(..) {
                path.retire(accounting)?;
            }
            selected.retire(accounting)?;
            self.select_fork_file_table(
                root,
                overrides,
                DecodeLimits::for_volume(config),
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        readonly_cleanup_standalone(accounting, baseline, 0)?;
        result
    }

    async fn select_fork_file_table(
        &self,
        root: &GenerationRoot,
        mut overrides: Owned<Vec<FileRecord>>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        crate::kernel::sort_by_with_accounting(&mut overrides.value, accounting, |left, right| {
            left.file_id.cmp(&right.file_id)
        })?;
        let mut index = crate::kernel::allocation::fixed_index::FixedIndex::<usize>::default();
        let mut pending = Owned::<Vec<FileId>>::allocate(0, accounting)?;
        selection_push(&mut pending, root.root_file_id, accounting)?;
        let mut records = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
        while let Some(id) = pending.value.pop() {
            if let Some(position) = index.get(id.into_bytes(), accounting)?.copied() {
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<u64>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let record = records
                    .value
                    .get_mut(position)
                    .ok_or(FsError::InvalidDiff)?;
                record.link_count = record
                    .link_count
                    .checked_add(1)
                    .ok_or(WorkError::Overflow)?;
                continue;
            }
            let record = crate::kernel::binary_search_with_accounting(
                &overrides.value,
                accounting,
                |entry| entry.file_id.cmp(&id),
            )?
            .ok()
            .map(|position| overrides.value[position]);
            let mut record = if let Some(record) = record {
                record
            } else {
                let record = {
                    let (record, _allocation) = lookup_file_record_async_with_accounting(
                        &self.inner.objects,
                        root.file_table,
                        id,
                        limits,
                        accounting,
                        cancellation,
                        None,
                    )
                    .await?;
                    record
                };
                record.ok_or(WorkspaceError::NotFound)?
            };
            record.link_count = 1;
            let position = records.value.len();
            selection_push(&mut records, record, accounting)?;
            index.get_or_try_insert_with(id.into_bytes(), accounting, |_| Ok(position))?;
            if let FilePayload::Directory { entries } = record.payload {
                let mut cursor: Option<Owned<LogicalName>> = None;
                loop {
                    let (page, allocation) = list_tree_entries_async_with_accounting(
                        &self.inner.objects,
                        entries,
                        (
                            (cursor.as_ref().map(|cursor| &cursor.value))
                                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            limits.maximum_page_items,
                        ),
                        limits,
                        accounting,
                        cancellation,
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
                        selection_push(&mut pending, entry.file_id, accounting)?;
                    }
                    let has_more = page.value.has_more;
                    let next = if has_more {
                        let name = page.value.entries.pop().ok_or(FsError::InvalidDiff)?.name;
                        let bytes = name.retained_bytes() as u64;
                        let allocation =
                            crate::kernel::split_allocation(&mut page.allocation, bytes)?;
                        page.bytes = page.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
                        Some(Owned::new(name, allocation, bytes))
                    } else {
                        None
                    };
                    if let Some(cursor) = cursor.take() {
                        cursor.retire(accounting)?;
                    }
                    cursor = next;
                    page.retire(accounting)?;
                    if !has_more {
                        break;
                    }
                }
            }
        }
        pending.retire(accounting)?;
        drop(index);
        overrides.retire(accounting)?;
        let mut mutations = Owned::<Vec<FileTableMutation>>::allocate(0, accounting)?;
        for record in &records.value {
            selection_push(
                &mut mutations,
                FileTableMutation::Insert(*record),
                accounting,
            )?;
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
            limits,
            accounting,
            cancellation,
        )
        .await?;
        mutations.retire(accounting)?;
        Ok(table.value)
    }
}
