//! Bounded immutable namespace selection.

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

fn selection_prefix(
    path: &[LogicalName],
    prefix: &[LogicalName],
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, WorkspaceError> {
    accounting.charge(WorkCounters {
        items_examined: 1 + path.len().min(prefix.len()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(path.starts_with(prefix))
}

pub(super) fn selection_push<T>(
    values: &mut Owned<Vec<T>>,
    value: T,
    maximum: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    if values.value.len() >= maximum {
        return Err(WorkspaceError::JoinLimit);
    }
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
    values.ensure_for_push(maximum, accounting)?;
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
            let (expected, _) = self
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
            let maximum = config.limits.maximum_mutations_per_batch as usize;
            if paths.value.len() > maximum {
                return Err(WorkspaceError::JoinLimit);
            }
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
            let mut selected = Owned::<Vec<Owned<NamespacePath>>>::allocate(0, accounting)?;
            for path in &paths.value {
                let (path, allocation) =
                    customer_path_with_accounting(path, config, accounting, cancellation)?;
                let bytes = path.retained_bytes();
                selection_push(
                    &mut selected,
                    Owned::new(path, allocation, bytes),
                    maximum,
                    accounting,
                )?;
            }
            paths.retire(accounting)?;
            if maximum == 0 {
                return Err(WorkspaceError::JoinLimit);
            }
            // Each kernel step runs in a paid heap frame; this frame holds none.
            let mut checkout = crate::heap_future::metered!(erased accounting, |accounting| source
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(source.id),
                    CheckoutMode::read_only_pinned(),
                    accounting,
                    cancellation,
                ))?;
            let mut overrides = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
            let mut directory_count = 0;
            // Root is always pruned; other prefixes are visited once, outside selected subtrees.
            for index in 0..=selected.value.len() {
                let depth_limit = selected
                    .value
                    .get(index)
                    .map_or(1, |path| path.value.depth());
                for depth in 0..depth_limit {
                    if depth == 0 && index != selected.value.len() {
                        continue;
                    }
                    let components = selected
                        .value
                        .get(index)
                        .map_or(&[][..], |path| path.value.components());
                    let prefix = components.get(..depth).ok_or(WorkspaceError::JoinLimit)?;
                    if depth != 0 {
                        let mut skip = false;
                        for (prior_index, prior) in selected.value.iter().enumerate() {
                            skip |= selection_prefix(prefix, prior.value.components(), accounting)?
                                || (prior_index < index
                                    && prior.value.depth() > depth
                                    && selection_prefix(prior.value.components(), prefix, accounting)?);
                        }
                        if skip {
                            continue;
                        }
                    }
                    directory_count += 1;
                    if directory_count > maximum {
                        return Err(WorkspaceError::JoinLimit);
                    }
                    let (directory, allocation) = if depth == 0 {
                        (
                            NamespacePath::new(Vec::new(), config.limits)?,
                            accounting.claim_owned(0, 0)?,
                        )
                    } else {
                        selected.value.get(index).ok_or(WorkspaceError::JoinLimit)?.value.prefix_with_accounting(
                            depth,
                            config.limits,
                            accounting,
                            cancellation,
                        )?
                    };
                    let bytes = directory.retained_bytes();
                    let directory = Owned::new(directory, allocation, bytes);
                    let lookup = {
                        let (lookup, _allocation) =
                            crate::heap_future::metered!(erased accounting, |accounting| checkout
                                .value
                                .lookup_no_follow_with_accounting(
                                    &directory.value,
                                    accounting,
                                    cancellation,
                                    false,
                                ))?;
                        lookup
                    };
                    let original = lookup.record.ok_or(WorkspaceError::NotFound)?;
                    if original.kind != FileKind::Directory {
                        return Err(WorkspaceError::NotDirectory);
                    }
                    let mut mutations = Owned::<Vec<TreeMutation>>::allocate(0, accounting)?;
                    for path in &selected.value {
                        if path.value.depth() <= depth
                            || !selection_prefix(path.value.components(), prefix, accounting)?
                        {
                            continue;
                        }
                        let name = path.value.components().get(depth).ok_or(WorkspaceError::JoinLimit)?;
                        let mut duplicate = false;
                        for mutation in &mutations.value {
                            accounting.charge(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                            if matches!(mutation, TreeMutation::Insert(entry) if &entry.name == name) {
                                duplicate = true;
                                break;
                            }
                        }
                        if duplicate {
                            continue;
                        }
                        let (child, allocation) = path.value.prefix_with_accounting(
                            depth + 1,
                            config.limits,
                            accounting,
                            cancellation,
                        )?;
                        let bytes = child.retained_bytes();
                        let child = Owned::new(child, allocation, bytes);
                        let lookup = {
                            let (lookup, _allocation) =
                                crate::heap_future::metered!(erased accounting, |accounting| checkout
                                    .value
                                    .lookup_no_follow_with_accounting(
                                        &child.value,
                                        accounting,
                                        cancellation,
                                        false,
                                    ))?;
                            lookup
                        };
                        child.retire(accounting)?;
                        let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
                        let (name, mut allocation) =
                            name.clone_with_accounting(accounting, cancellation)?;
                        let bytes = name.retained_bytes() as u64;
                        // Retain the name guard before any fallible vector growth.
                        mutations.allocation.absorb(&mut allocation)?;
                        mutations.bytes = mutations
                            .bytes
                            .checked_add(bytes)
                            .ok_or(WorkError::Overflow)?;
                        selection_push(
                            &mut mutations,
                            TreeMutation::Insert(TreeEntry {
                                name,
                                file_id: record.file_id,
                                kind: record.kind,
                            }),
                            maximum,
                            accounting,
                        )?;
                    }
                    directory.retire(accounting)?;
                    let empty = crate::heap_future::metered!(erased accounting, |accounting| checkout
                        .value
                        .stage_empty_tree_with_accounting(accounting, cancellation))?;
                    let entries = if mutations.value.is_empty() {
                        empty
                    } else {
                        let limit = u32::try_from(maximum).map_err(|_| WorkspaceError::JoinLimit)?;
                        crate::heap_future::metered!(erased accounting, |accounting| {
                            apply_tree_mutations_with_accounting(
                                &self.inner.objects,
                                empty,
                                &mutations.value,
                                limit,
                                DecodeLimits::for_volume(config),
                                accounting,
                                cancellation,
                            )
                        })?
                        .value
                    };
                    mutations.retire(accounting)?;
                    let record = FileRecord {
                        payload: FilePayload::Directory { entries },
                        ..original
                    };
                    selection_push(&mut overrides, record, maximum, accounting)?;
                }
            }
            checkout.retire(accounting)?;
            for path in selected.value.drain(..) {
                path.retire(accounting)?;
            }
            selected.retire(accounting)?;
            crate::heap_future::metered!(erased accounting, |accounting| self
                .select_fork_file_table(
                    root,
                    overrides,
                    DecodeLimits::for_volume(config),
                    maximum,
                    accounting,
                    cancellation,
                ))
        }
        .await;
        readonly_cleanup_standalone(accounting, baseline, 0)?;
        result
    }

    async fn select_fork_file_table(
        &self,
        root: &GenerationRoot,
        overrides: Owned<Vec<FileRecord>>,
        limits: DecodeLimits,
        maximum: usize,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, WorkspaceError> {
        let mut pending = Owned::<Vec<FileId>>::allocate(0, accounting)?;
        selection_push(&mut pending, root.root_file_id, maximum, accounting)?;
        let mut records = Owned::<Vec<FileRecord>>::allocate(0, accounting)?;
        while let Some(id) = pending.value.pop() {
            let position = crate::kernel::binary_search_with_accounting(
                &records.value,
                accounting,
                |entry| entry.file_id.cmp(&id),
            )?;
            let position = match position {
                Ok(position) => {
                    let record = records
                        .value
                        .get_mut(position)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    record.link_count = record
                        .link_count
                        .checked_add(1)
                        .ok_or(WorkspaceError::JoinLimit)?;
                    continue;
                }
                Err(position) => position,
            };
            let mut record = None;
            for entry in &overrides.value {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if entry.file_id == id {
                    record = Some(*entry);
                    break;
                }
            }
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
            selection_push(&mut records, record, maximum, accounting)?;
            accounting.charge(WorkCounters {
                bytes_copied: ((records.value.len() - position) * size_of::<FileRecord>()) as u64,
                ..WorkCounters::UNCHARGED
            })?;
            records
                .value
                .get_mut(position..)
                .ok_or(WorkspaceError::JoinLimit)?
                .rotate_right(1);
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
                        selection_push(&mut pending, entry.file_id, maximum, accounting)?;
                    }
                    let has_more = page.value.has_more;
                    let next = if has_more {
                        let name = page
                            .value
                            .entries
                            .pop()
                            .ok_or(WorkspaceError::JoinLimit)?
                            .name;
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
        overrides.retire(accounting)?;
        let mut mutations = Owned::<Vec<FileTableMutation>>::allocate(0, accounting)?;
        for record in &records.value {
            selection_push(
                &mut mutations,
                FileTableMutation::Insert(*record),
                maximum,
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
            u32::try_from(maximum).map_err(|_| WorkspaceError::JoinLimit)?,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        mutations.retire(accounting)?;
        Ok(table.value)
    }
}
