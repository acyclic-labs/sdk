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
    // Namespace order and file-identity order are independent. Append during
    // discovery; sorting and grouping once avoids shifting every prior file.
    reverse_grow(records, allocation, accounting)?;
    let mut entries = Vec::new();
    reverse_grow(&mut entries, allocation, accounting)?;
    let mut path = path;
    accounting.charge(copy_work(
        (std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()
            + std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>())
            as u64,
    ))?;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
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
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId,
    ids: &[FileId],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut matches = (
        NamespaceRecordSearch {
            records: NamespaceRecords::default(),
            complete: ids.is_empty(),
        },
        allocation,
    );
    if ids.is_empty() || maximum_entries == 0 {
        return Ok(matches);
    }
    let limits = volume.config.limits;
    let generation = GenerationId::new(generation_root.digest);
    let cache_key = crate::path_index::cache_key(
        generation,
        root.root_file_id,
        root.file_table,
        ids,
        accounting,
    )?;
    let (bytes, allocation) = volume.fs.path_index().load(cache_key, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = bytes.0.as_ref() {
        let cached = crate::path_index::decode(payload, generation, ids, limits, accounting)?;
        if answer_from_cache(
            volume,
            root,
            maximum_entries,
            cached,
            &mut matches,
            accounting,
            cancellation,
        )
        .await?
        {
            let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
            reverse_drop(bytes, bytes_count, accounting)?;
            return Ok(matches);
        }
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let view = super::namespace_view::NamespaceView {
        objects: volume.fs.objects(),
        config: volume.config(),
        root,
    };
    if !collect_namespace(
        &view,
        Some(ids),
        maximum_entries,
        &mut matches,
        accounting,
        cancellation,
    )
    .await?
    {
        return Ok(matches);
    }
    let (bytes, allocation) =
        crate::path_index::encode_records(generation, ids, &matches.0.records, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = &bytes.0 {
        volume
            .fs
            .path_index()
            .store(cache_key, payload, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    matches.0.complete = true;
    Ok(matches)
}

/// Answers from a decoded path index entry when every cached path still
/// binds the identity it recorded, inserting those paths into `matches`.
/// Returns whether `matches` now holds the complete answer.
async fn answer_from_cache<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    maximum_entries: u32,
    mut cached: crate::kernel::Owned<Option<Vec<(FileId, NamespacePath)>>>,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<bool, FsError> {
    // A complete traversal that reached none of the identities is cached
    // as an empty answer; there is no path left to revalidate.
    if cached.value.as_ref().is_some_and(Vec::is_empty) {
        cached.retire(accounting)?;
        matches.0.complete = true;
        return Ok(true);
    }
    if let Some(paths) = cached
        .value
        .as_ref()
        .filter(|paths| paths.len() <= maximum_entries as usize)
    {
        let allocation = accounting.claim_owned(0, 0)?;
        let mut copied = (Vec::new(), allocation);
        for (_, path) in paths {
            reverse_grow(&mut copied.0, &mut copied.1, accounting)?;
            let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
            let mut path = (path, allocation);
            copied.1.absorb(&mut path.1)?;
            copied.0.push(path.0);
        }
        let (lookup, allocation) = crate::kernel::lookup_paths_with_accounting(
            volume.fs.objects(),
            root,
            crate::kernel::PathQueries::Owned(&copied.0),
            volume.config(),
            accounting,
            cancellation,
            None,
            crate::kernel::PathBatchMode::NoFollow,
        )
        .await?;
        let lookup = (lookup, allocation);
        let mut valid = paths.len() == lookup.0.entries.len();
        for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
            accounting.charge_items(1)?;
            if entry
                .record
                .is_none_or(|record| record.file_id != *expected)
            {
                valid = false;
                break;
            }
        }
        let copied_bytes = workspace_vec_bytes(&copied.0)
            + copied
                .0
                .iter()
                .map(NamespacePath::retained_bytes)
                .sum::<u64>();
        reverse_drop(copied, copied_bytes, accounting)?;
        let lookup_bytes = lookup.0.retained_allocation_bytes;
        if valid {
            let paths = cached.value.take().ok_or(FsError::InvalidDiff)?;
            let vector_bytes = workspace_vec_bytes(&paths);
            for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                let allocation =
                    crate::kernel::split_allocation(&mut cached.allocation, path.retained_bytes())?;
                let path = (path, allocation);
                let record = entry.record.ok_or(FsError::NotFound)?;
                reverse_insert(
                    &mut matches.0.records,
                    &mut matches.1,
                    path,
                    record,
                    accounting,
                )?;
            }
            accounting.release_owned(&mut cached.allocation, vector_bytes)?;
            reverse_drop(lookup, lookup_bytes, accounting)?;
            reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
            matches.0.complete = true;
            return Ok(true);
        }
        reverse_drop(lookup, lookup_bytes, accounting)?;
    }
    cached.retire(accounting)?;
    Ok(false)
}

pub(crate) async fn collect_namespace<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    ids: Option<&[FileId]>,
    maximum_entries: u32,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    // Closure validation authenticates each record's exact namespace link
    // count. Once every requested binding has been found, unrelated subtrees
    // cannot contain another alias and need not be traversed.
    let mut remaining = if let Some(ids) = ids {
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
        let owned = (records, allocation);
        let mut count = 0_u64;
        for record in &owned.0 {
            accounting.charge_items(1)?;
            count = count
                .checked_add(record.as_ref().map_or(0, |record| record.link_count))
                .ok_or(crate::WorkError::Overflow)?;
        }
        let bytes = workspace_vec_bytes(&owned.0);
        reverse_drop(owned, bytes, accounting)?;
        Some(count)
    } else {
        None
    };
    if remaining == Some(0) {
        reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
        return Ok(true);
    }
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
        && namespace_selected(ids, record.file_id, accounting)?
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
        if let Some(remaining) = remaining.as_mut() {
            *remaining = remaining.checked_sub(1).ok_or(FsError::InvalidDiff)?;
        }
    }
    if remaining == Some(0) {
        reverse_sort(&mut matches.0.records, &mut matches.1, accounting)?;
        return Ok(true);
    }
    let pending = initial_walk(limits, accounting)?;
    walk_namespace(
        view,
        WalkSelection {
            ids,
            remaining,
            maximum_entries,
        },
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
        WalkSelection {
            ids: None,
            remaining: None,
            maximum_entries,
        },
        pending,
        matches,
        accounting,
        cancellation,
    )
    .await
}

struct WalkSelection<'a> {
    ids: Option<&'a [FileId]>,
    remaining: Option<u64>,
    maximum_entries: u32,
}

/// Walks only the supplied pending subtrees. Returns false if the entry bound
/// prevents examining every entry needed by the selection.
async fn walk_namespace<O: AsyncObjectStore>(
    view: &super::namespace_view::NamespaceView<'_, O>,
    selection: WalkSelection<'_>,
    mut pending: PendingDirectories,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<bool, FsError> {
    let limits = view.config.limits;
    let WalkSelection {
        ids,
        mut remaining,
        maximum_entries,
    } = selection;
    let mut examined = 0_u32;
    while let Some(directory) = pending.0.pop() {
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) = view
                .list(
                    &directory.0,
                    after.as_ref().map(|name| &name.0),
                    ids,
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
                let found = namespace_selected(ids, record.file_id, accounting)?;
                let is_directory = record.kind == FileKind::Directory;
                if found && let Some(remaining) = remaining.as_mut() {
                    *remaining = remaining.checked_sub(1).ok_or(FsError::InvalidDiff)?;
                }
                if !found && !is_directory {
                    continue;
                }
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name,
                    limits,
                    accounting,
                    cancellation,
                )?;
                let path = (path, allocation);
                if found && is_directory {
                    let (copy, allocation) =
                        path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        (copy, allocation),
                        record,
                        accounting,
                    )?;
                } else if found {
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        path,
                        record,
                        accounting,
                    )?;
                    if remaining == Some(0) {
                        finish_walk_page(matches, page, accounting)?;
                        abandon_walk(after, directory, pending, accounting)?;
                        return Ok(true);
                    }
                    continue;
                }
                if remaining == Some(0) {
                    let bytes = path.0.retained_bytes();
                    reverse_drop(path, bytes, accounting)?;
                    finish_walk_page(matches, page, accounting)?;
                    abandon_walk(after, directory, pending, accounting)?;
                    return Ok(true);
                }
                enqueue_directory(&mut pending, path, found && ids.is_some(), accounting)?;
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

// Already located directory identities remain eligible for descendant matches,
// but unresolved sibling directories are searched first. Otherwise finding a
// large retained subtree can force a complete walk before a nearby sibling.
fn enqueue_directory(
    pending: &mut (
        Vec<(NamespacePath, KernelOwnedAllocation)>,
        KernelOwnedAllocation,
    ),
    path: (NamespacePath, KernelOwnedAllocation),
    defer: bool,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    if defer {
        let bytes = pending
            .0
            .len()
            .checked_mul(std::mem::size_of::<(NamespacePath, KernelOwnedAllocation)>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::WorkError::Overflow)?;
        accounting.charge(copy_work(bytes))?;
        pending.0.insert(0, path);
    } else {
        pending.0.push(path);
    }
    Ok(())
}

fn namespace_selected(
    ids: Option<&[FileId]>,
    file_id: FileId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    match ids {
        Some(ids) => binary_search_with_accounting(ids, accounting, |id| id.cmp(&file_id))
            .map(|position| position.is_ok()),
        None => Ok(true),
    }
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
