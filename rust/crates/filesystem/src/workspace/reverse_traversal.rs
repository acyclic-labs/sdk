use super::*;

pub(super) fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge(item_work(1))?;
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
    let position =
        binary_search_with_accounting(records, accounting, |entry| entry.0.cmp(&record.file_id))?;
    let (Ok(index) | Err(index)) = position;
    if position.is_err() {
        reverse_grow(records, allocation, accounting)?;
        accounting.charge(copy_work(
            ((records.len() - index)
                * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>())
                as u64,
        ))?;
        records.insert(index, (record.file_id, Vec::new()));
    }
    let entries = &mut records[index].1;
    reverse_grow(entries, allocation, accounting)?;
    let mut path = path;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
    Ok(())
}

fn reverse_sort(
    records: &mut NamespaceRecords,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    for (_, entries) in records {
        for index in 1..entries.len() {
            let mut current = index;
            while current != 0 {
                accounting.charge(item_work(1))?;
                if entries[current - 1].0 <= entries[current].0 {
                    break;
                }
                accounting.charge(copy_work(
                    (2 * std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()) as u64,
                ))?;
                entries.swap(current - 1, current);
                current -= 1;
            }
        }
    }
    Ok(())
}

pub(super) async fn namespace_records_to_file_root_with_accounting<
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
        let (cached, allocation) =
            crate::path_index::decode(payload, generation, ids, limits, accounting)?;
        let mut cached = (cached, allocation);
        if let Some(paths) = cached
            .0
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
            let (lookup, allocation) = crate::kernel::lookup_paths_async_with_accounting(
                volume.fs.objects(),
                root,
                &copied.0,
                volume.config(),
                accounting,
                cancellation,
                None,
            )
            .await?
            .into_parts();
            let lookup = (lookup, allocation);
            let mut valid = paths.len() == lookup.0.entries.len();
            for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
                accounting.charge(item_work(1))?;
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
                let paths = cached.0.take().expect("verified cache payload");
                let vector_bytes = workspace_vec_bytes(&paths);
                for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                    let allocation =
                        crate::kernel::split_allocation(&mut cached.1, path.retained_bytes())?;
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
                accounting.release_owned(&mut cached.1, vector_bytes)?;
                reverse_drop(lookup, lookup_bytes, accounting)?;
                matches.0.complete = true;
                let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
                reverse_drop(bytes, bytes_count, accounting)?;
                return Ok(matches);
            }
            reverse_drop(lookup, lookup_bytes, accounting)?;
        }
        let cached_bytes = cached.0.as_ref().map_or(0, |paths| {
            workspace_vec_bytes(paths)
                + paths
                    .iter()
                    .map(|(_, path)| path.retained_bytes())
                    .sum::<u64>()
        });
        reverse_drop(cached, cached_bytes, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::kernel::lookup_path_async_with_accounting(
        volume.fs.objects(),
        root,
        &namespace_root,
        volume.config(),
        accounting,
        cancellation,
        None,
    )
    .await?
    .record
    {
        if binary_search_with_accounting(ids, accounting, |id| id.cmp(&record.file_id))?.is_ok() {
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
    }
    let allocation = accounting.claim_owned(0, 0)?;
    let mut pending = (Vec::new(), allocation);
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    let allocation = accounting.claim_owned(0, 0)?;
    pending.0.push((namespace_root, allocation));
    let mut examined = 0_u32;
    while let Some(directory) = pending.0.pop() {
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) =
                crate::facade::list_directory_records_to_file_root_with_accounting(
                    volume,
                    root,
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
            let mut page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge(item_work(1))?;
                if examined == maximum_entries {
                    reverse_sort(&mut matches.0.records, accounting)?;
                    let page_bytes = workspace_vec_bytes(&page.0.entries)
                        + page
                            .0
                            .entries
                            .iter()
                            .map(|entry| entry.name.retained_bytes() as u64)
                            .sum::<u64>();
                    reverse_drop(page, page_bytes, accounting)?;
                    if let Some(name) = after.take() {
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
                    reverse_drop(pending, bytes, accounting)?;
                    return Ok(matches);
                }
                examined += 1;
                let found = binary_search_with_accounting(ids, accounting, |id| {
                    id.cmp(&entry.record.file_id)
                })?
                .is_ok();
                let is_directory = entry.record.kind == FileKind::Directory;
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
                        entry.record,
                        accounting,
                    )?;
                } else if found {
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        path,
                        entry.record,
                        accounting,
                    )?;
                    continue;
                }
                reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
                pending.0.push(path);
            }
            let next = if page.0.has_more {
                let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
                let allocation =
                    crate::kernel::split_allocation(&mut page.1, name.retained_bytes() as u64)?;
                Some((name, allocation))
            } else {
                None
            };
            let page_bytes = workspace_vec_bytes(&page.0.entries)
                + page
                    .0
                    .entries
                    .iter()
                    .map(|entry| entry.name.retained_bytes() as u64)
                    .sum::<u64>();
            reverse_drop(page, page_bytes, accounting)?;
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                reverse_drop(old, bytes, accounting)?;
            }
            after = next;
            if after.is_none() {
                break;
            }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, accounting)?;
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

/// Reverse aliases of the actual supplied root, retaining their original allocation.
pub(crate) async fn reverse_paths_to_file_root_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId,
    file_id: FileId,
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(Vec<NamespacePath>, KernelOwnedAllocation), WorkspaceError> {
    let mut search = namespace_records_to_file_root_with_accounting(
        volume,
        root,
        generation_root,
        &[file_id],
        maximum_entries,
        accounting,
        cancellation,
    )
    .await?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut paths = (Vec::new(), allocation);
    let entries_bytes = workspace_vec_bytes(&search.0.records);
    for (_, records) in std::mem::take(&mut search.0.records) {
        let bytes = workspace_vec_bytes(&records);
        for (path, _) in records {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            paths.0.push(path);
        }
        accounting.release_owned(&mut search.1, bytes)?;
    }
    accounting.release_owned(&mut search.1, entries_bytes)?;
    paths.1.absorb(&mut search.1)?;
    Ok((paths.0, paths.1))
}

pub(super) struct NamespaceRecordSearch {
    pub(super) records: NamespaceRecords,
    pub(super) complete: bool,
}

pub(super) type NamespaceRecords = Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>;

pub(super) fn namespace_records_bytes(records: &NamespaceRecords) -> u64 {
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
