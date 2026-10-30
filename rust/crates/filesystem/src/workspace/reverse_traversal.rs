use super::*;

pub(super) fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge_items(1)?;
        let Err(low) = binary_search_with_accounting(&ids.0, accounting, |id| id.cmp(&file_id))?
        else {
            continue;
        };
        reverse_grow(&mut ids.0, &mut ids.1, accounting)?;
        let copied = copy_work(((ids.0.len() - low + 1) * std::mem::size_of::<FileId>()) as u64);
        accounting.admit(copied)?;
        ids.0.insert(low, file_id);
        accounting.record_completed(copied)?;
    }
    Ok(ids)
}

pub(super) fn reverse_drop<T>(
    owned: (T, KernelOwnedAllocation),
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let (value, mut allocation) = owned;
    drop(value);
    accounting.release_owned(&mut allocation, bytes)
}

pub(super) fn reverse_grow<T>(
    values: &mut Vec<T>,
    allocation: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let copied = copy_work(
        values
            .len()
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::WorkError::Overflow)?,
    );
    accounting.admit(copied)?;
    let bytes = workspace_vec_bytes(values);
    let mut owned = crate::kernel::Owned::new(
        std::mem::take(values),
        std::mem::replace(allocation, KernelOwnedAllocation::Standalone),
        bytes,
    );
    let result = owned.ensure_for_push(usize::MAX, accounting);
    *values = owned.value;
    *allocation = owned.allocation;
    result?;
    accounting.record_completed(copied)
}

fn reverse_insert(
    records: &mut NamespaceRecords,
    allocation: &mut KernelOwnedAllocation,
    path: (NamespacePath, KernelOwnedAllocation),
    record: crate::kernel::FileRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    // The path stays with its original guard until every destination is funded.
    let bytes = path.0.retained_bytes();
    let path = crate::kernel::Owned::new(path.0, path.1, bytes);
    let prepared = reverse_grow(records, allocation, accounting).and_then(|()| {
        accounting.charge(copy_work(
            (std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()
                + std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>())
                as u64,
        ))
    });
    if let Err(error) = prepared {
        path.retire(accounting)?;
        return Err(error);
    }
    let mut entries = match crate::kernel::Owned::<Vec<_>>::allocate(1, accounting) {
        Ok(entries) => entries,
        Err(error) => {
            path.retire(accounting)?;
            return Err(error);
        }
    };
    let path = match path.transfer_to(&mut entries.allocation, &mut entries.bytes, accounting) {
        Ok(path) => path,
        Err(error) => {
            entries.retire(accounting)?;
            return Err(error);
        }
    };
    entries.value.push((path, record));
    let mut transferred = 0;
    let entries = entries.transfer_to(allocation, &mut transferred, accounting)?;
    records.push((record.file_id, entries));
    Ok(())
}

pub(super) fn reverse_sort(
    records: &mut NamespaceRecords,
    allocation: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    crate::kernel::sort_by_with_accounting(records, accounting, |left, right| {
        left.0.cmp(&right.0)
    })?;
    let mut retained = 0_usize;
    for index in 0..records.len() {
        accounting.charge_items(1)?;
        let previous = retained.checked_sub(1);
        let duplicate = previous.is_some_and(|previous| {
            records.get(previous).map(|entry| entry.0) == records.get(index).map(|entry| entry.0)
        });
        if duplicate {
            merge_reverse_group(
                records,
                previous.ok_or(FsError::InvalidDiff)?,
                index,
                allocation,
                accounting,
            )?;
        } else {
            if retained != index {
                accounting.charge(copy_work(
                    (2 * std::mem::size_of::<(
                        FileId,
                        Vec<(NamespacePath, crate::kernel::FileRecord)>,
                    )>()) as u64,
                ))?;
                records.swap(retained, index);
            }
            retained += 1;
        }
    }
    records.truncate(retained);
    for (_, entries) in records {
        crate::kernel::sort_by_with_accounting(entries, accounting, |left, right| {
            left.0.cmp(&right.0)
        })?;
    }
    Ok(())
}

fn merge_reverse_group(
    records: &mut NamespaceRecords,
    target: usize,
    source: usize,
    allocation: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let mut source = std::mem::take(&mut records.get_mut(source).ok_or(FsError::InvalidDiff)?.1);
    let bytes = workspace_vec_bytes(&source);
    let entries = &mut records.get_mut(target).ok_or(FsError::InvalidDiff)?.1;
    for entry in source.drain(..) {
        reverse_grow(entries, allocation, accounting)?;
        accounting.charge(copy_work(
            std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>() as u64,
        ))?;
        entries.push(entry);
    }
    drop(source);
    accounting.release_owned(allocation, bytes)
}

pub(crate) async fn namespace_records_to_file_root_with_accounting<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    ids: &[FileId],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    super::namespace_view::NamespaceView {
        objects: volume.fs.objects(),
        config: volume.config(),
        root,
    }
    .records(ids, maximum_entries, accounting, cancellation)
    .await
}

pub(super) async fn collect_selected<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    ids: &[FileId],
    maximum_entries: u32,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let (records, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
        view.objects,
        view.root.file_table,
        ids,
        u32::try_from(ids.len()).map_err(|_| crate::WorkError::Overflow)?,
        crate::kernel::DecodeLimits::for_volume(view.config),
        accounting,
        cancellation,
        None,
    )
    .await?;
    let bytes = workspace_vec_bytes(&records);
    let records = crate::kernel::Owned::new(records, allocation, bytes);
    let result = async {
        let mut emitted = 0_u32;
        for record in records.value.iter().flatten().copied() {
            let mut paths = crate::kernel::direct_paths::DirectPaths::new(
                view.root,
                record,
                view.config,
                accounting,
            )?;
            let result = async {
                while let Some(path) = paths.next(view.objects, accounting, cancellation).await? {
                    if emitted == maximum_entries {
                        path.retire(accounting)?;
                        return Ok(false);
                    }
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        (path.value, path.allocation),
                        record,
                        accounting,
                    )?;
                    emitted += 1;
                }
                Ok(true)
            }
            .await;
            let retired = paths.retire(accounting);
            if !result.and_then(|complete| retired.map(|()| complete))? {
                reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
                return Ok(false);
            }
        }
        reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
        Ok(true)
    }
    .await;
    let retired = records.retire(accounting);
    result.and_then(|complete| retired.map(|()| complete))
}

pub(crate) async fn collect_namespace<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    maximum_entries: u32,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let limits = view.config.limits;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::kernel::lookup_path_async_with_accounting(
        view.objects,
        view.root,
        &namespace_root,
        view.config,
        accounting,
        cancellation,
        None,
    )
    .await?
    .record
    {
        let allocation = accounting.claim_owned(0, 0)?;
        reverse_insert(
            &mut matches.0.records,
            &mut matches.1,
            (
                NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?,
                allocation,
            ),
            record,
            accounting,
        )?;
    }
    let pending = initial_walk(limits, accounting)?;
    walk_namespace(
        view,
        maximum_entries,
        pending,
        matches,
        accounting,
        cancellation,
    )
    .await
}

pub(super) async fn collect_descendants<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    path: &NamespacePath,
    maximum_entries: u32,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let mut pending = (Vec::new(), accounting.claim_owned(0, 0)?);
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    pending
        .0
        .push(path.clone_with_accounting(accounting, cancellation)?);
    walk_namespace(
        view,
        maximum_entries,
        pending,
        matches,
        accounting,
        cancellation,
    )
    .await
}

/// Walks only the supplied pending subtrees. Returns false if the entry bound
/// prevents examining every entry needed by the selection.
async fn walk_namespace<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    maximum_entries: u32,
    mut pending: PendingDirectories,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<bool, FsError> {
    let limits = view.config.limits;
    let mut examined = 0_u32;
    while let Some(directory) = pending.0.pop() {
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) = view
                .list(
                    &directory.0,
                    after.as_ref().map(|name| &name.0),
                    maximum_entries
                        .saturating_sub(examined)
                        .saturating_add(1)
                        .min(1_024),
                    accounting,
                    cancellation,
                )
                .await?;
            let page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge_items(1)?;
                if examined == maximum_entries {
                    finish_walk_page(matches, page, accounting)?;
                    abandon_walk(after, directory, pending, accounting)?;
                    return Ok(false);
                }
                examined += 1;
                let Some(record) = entry.record else {
                    continue;
                };
                let is_directory = record.kind == FileKind::Directory;
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name,
                    limits,
                    accounting,
                    cancellation,
                )?;
                let path = (path, allocation);
                if is_directory {
                    let (copy, allocation) =
                        path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        (copy, allocation),
                        record,
                        accounting,
                    )?;
                } else {
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        path,
                        record,
                        accounting,
                    )?;
                    continue;
                }
                enqueue_directory(&mut pending, path, accounting)?;
            }
            after = advance_walk_page(page, after, accounting)?;
            if after.is_none() {
                break;
            }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
    Ok(true)
}

// Every bounded exit authenticates the same canonical ordering before the
// completed page is retired. Remaining walk state is then abandoned by caller.
fn finish_walk_page(
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    page: (super::namespace_view::NamespacePage, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
    drop_page(page, accounting)
}

fn enqueue_directory(
    pending: &mut (
        Vec<(NamespacePath, KernelOwnedAllocation)>,
        KernelOwnedAllocation,
    ),
    path: (NamespacePath, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    pending.0.push(path);
    Ok(())
}

/// Releases one listed directory page.
fn drop_page(
    page: (super::namespace_view::NamespacePage, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let page_bytes = workspace_vec_bytes(&page.0.entries)
        + page
            .0
            .entries
            .iter()
            .map(|entry| entry.name.retained_bytes() as u64)
            .sum::<u64>();
    reverse_drop(page, page_bytes, accounting)
}

/// Releases a walk stopped before it finished: the listing cursor, the
/// directory being listed, and every directory still pending.
fn abandon_walk(
    after: Option<(LogicalName, KernelOwnedAllocation)>,
    directory: (NamespacePath, KernelOwnedAllocation),
    mut pending: (
        Vec<(NamespacePath, KernelOwnedAllocation)>,
        KernelOwnedAllocation,
    ),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if let Some(name) = after {
        let bytes = name.0.retained_bytes() as u64;
        reverse_drop(name, bytes, accounting)?;
    }
    let bytes = directory.0.retained_bytes();
    reverse_drop(directory, bytes, accounting)?;
    while let Some(path) = pending.0.pop() {
        let bytes = path.0.retained_bytes();
        reverse_drop(path, bytes, accounting)?;
    }
    let bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, bytes, accounting)
}

pub(crate) struct NamespaceRecordSearch {
    pub(crate) records: NamespaceRecords,
    pub(crate) complete: bool,
}

pub(super) type NamespaceRecords = Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>;

pub(crate) fn namespace_records_bytes(records: &NamespaceRecords) -> u64 {
    workspace_vec_bytes(records)
        + records
            .iter()
            .map(|(_, records)| {
                workspace_vec_bytes(records)
                    + records
                        .iter()
                        .map(|(path, _)| path.retained_bytes())
                        .sum::<u64>()
            })
            .sum::<u64>()
}

fn advance_walk_page(
    mut page: (super::namespace_view::NamespacePage, KernelOwnedAllocation),
    after: Option<(LogicalName, KernelOwnedAllocation)>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(LogicalName, KernelOwnedAllocation)>, FsError> {
    let next = if page.0.has_more {
        let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
        let allocation =
            crate::kernel::split_allocation(&mut page.1, name.retained_bytes() as u64)?;
        Some((name, allocation))
    } else {
        None
    };
    drop_page(page, accounting)?;
    if let Some(old) = after {
        let bytes = old.0.retained_bytes() as u64;
        reverse_drop(old, bytes, accounting)?;
    }
    Ok(next)
}

type PendingDirectories = (
    Vec<(NamespacePath, KernelOwnedAllocation)>,
    KernelOwnedAllocation,
);

fn initial_walk(
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<PendingDirectories, FsError> {
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut pending = (Vec::new(), allocation);
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    let allocation = accounting.claim_owned(0, 0)?;
    pending.0.push((namespace_root, allocation));
    Ok(pending)
}
