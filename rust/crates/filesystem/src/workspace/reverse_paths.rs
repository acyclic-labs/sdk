use super::*;

pub(super) fn bounded_search<T>(
    values: &[T], accounting: &mut KernelAccounting<'_>,
    mut compare: impl FnMut(&T, &mut KernelAccounting<'_>) -> Result<std::cmp::Ordering, FsError>,
) -> Result<(usize, bool), FsError> {
    let mut low = 0;
    let mut high = values.len();
    while low < high {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let middle = low + (high - low) / 2;
        match compare(&values[middle], accounting)? {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok((middle, true)),
        }
    }
    Ok((low, false))
}

pub(super) fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let (low, present) = bounded_search(&ids.0, accounting, |id, _| Ok(id.cmp(&file_id)))?;
        if present { continue; }
        reverse_grow(&mut ids.0, &mut ids.1, accounting)?;
        let copied = crate::WorkCounters {
            bytes_copied: ((ids.0.len() - low + 1) * std::mem::size_of::<FileId>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        ids.0.insert(low, file_id);
        accounting.record_completed(copied)?;
    }
    Ok(ids)
}

pub(super) fn reverse_drop<T>(owned: (T, KernelOwnedAllocation), bytes: u64, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let (value, mut allocation) = owned;
    drop(value);
    accounting.release_owned(&mut allocation, bytes)
}

pub(super) fn reverse_grow<T>(values: &mut Vec<T>, allocation: &mut KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    if values.len() < values.capacity() { return Ok(()); }
    let count = values.capacity().checked_mul(2).map(|count| count.max(1)).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters {
        bytes_copied: values.len().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    };
    let mut replacement = crate::kernel::Owned::<Vec<T>>::allocate(count, accounting)?;
    if let Err(error) = accounting.admit(copied) {
        replacement.retire(accounting)?;
        return Err(error);
    }
    if let Err(error) = allocation.absorb(&mut replacement.allocation) {
        replacement.retire(accounting)?;
        return Err(error);
    }
    let old_bytes = workspace_vec_bytes(values);
    replacement.value.append(values);
    let completed = accounting.record_completed(copied);
    let old = std::mem::replace(values, replacement.value);
    drop(old);
    accounting.release_owned(allocation, old_bytes)?;
    completed
}

fn reverse_insert(records: &mut NamespaceRecords, allocation: &mut KernelOwnedAllocation, path: (NamespacePath, KernelOwnedAllocation),
    record: crate::kernel::FileRecord, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let (index, present) = bounded_search(records, accounting, |entry, _| Ok(entry.0.cmp(&record.file_id)))?;
    if !present {
        reverse_grow(records, allocation, accounting)?;
        accounting.charge(crate::WorkCounters { bytes_copied: ((records.len() - index)
            * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>()) as u64,
            ..crate::WorkCounters::UNCHARGED })?;
        records.insert(index, (record.file_id, Vec::new()));
    }
    let entries = &mut records[index].1;
    reverse_grow(entries, allocation, accounting)?;
    let mut path = path;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
    Ok(())
}

fn reverse_sort(records: &mut NamespaceRecords, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    for (_, entries) in records {
        for index in 1..entries.len() {
            let mut current = index;
            while current != 0 {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entries[current - 1].0 <= entries[current].0 { break; }
                accounting.charge(crate::WorkCounters { bytes_copied:
                    (2 * std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()) as u64,
                    ..crate::WorkCounters::UNCHARGED })?;
                entries.swap(current - 1, current);
                current -= 1;
            }
        }
    }
    Ok(())
}

async fn namespace_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId, ids: &[FileId], maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut matches = (NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() }, allocation);
    if ids.is_empty() || maximum_entries == 0 { return Ok(matches); }
    let limits = volume.config.limits;
    let generation = GenerationId::new(generation_root.digest);
    let cache_key = crate::path_index::cache_key(
        generation, root.root_file_id, root.file_table, ids, accounting,
    )?;
    let (bytes, allocation) = volume.fs.path_index().load(cache_key, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = bytes.0.as_ref() {
        let (cached, allocation) = crate::path_index::decode(
            payload, generation, ids, limits, accounting,
        )?;
        let mut cached = (cached, allocation);
        if let Some(paths) = cached.0.as_ref().filter(|paths| paths.len() <= maximum_entries as usize) {
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
                volume.fs.objects(), root, &copied.0, volume.config(), accounting, cancellation, None,
            ).await?.into_parts();
            let lookup = (lookup, allocation);
            let mut valid = paths.len() == lookup.0.entries.len();
            for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entry.record.is_none_or(|record| record.file_id != *expected) { valid = false; break; }
            }
            let copied_bytes = workspace_vec_bytes(&copied.0)
                + copied.0.iter().map(NamespacePath::retained_bytes).sum::<u64>();
            reverse_drop(copied, copied_bytes, accounting)?;
            let lookup_bytes = lookup.0.retained_allocation_bytes;
            if valid {
                let paths = cached.0.take().expect("verified cache payload");
                let vector_bytes = workspace_vec_bytes(&paths);
                for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                    let allocation = crate::kernel::split_allocation(&mut cached.1, path.retained_bytes())?;
                    let path = (path, allocation);
                    let record = entry.record.ok_or(FsError::NotFound)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, record, accounting)?;
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
        let cached_bytes = cached.0.as_ref().map_or(0, |paths| workspace_vec_bytes(paths)
            + paths.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>());
        reverse_drop(cached, cached_bytes, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::kernel::lookup_path_async_with_accounting(
        volume.fs.objects(), root, &namespace_root, volume.config(), accounting, cancellation, None,
    ).await?.record {
        if bounded_search(ids, accounting, |id, _| Ok(id.cmp(&record.file_id)))?.1 {
            let allocation = accounting.claim_owned(0, 0)?;
            reverse_insert(&mut matches.0.records, &mut matches.1,
                (NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?, allocation), record, accounting)?;
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
            let (page, allocation) = crate::facade::list_directory_records_to_file_root_with_accounting(
                volume, root, &directory.0, after.as_ref().map(|name| &name.0),
                maximum_entries.saturating_sub(examined).saturating_add(1).min(1_024),
                accounting, cancellation,
            ).await?;
            let mut page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if examined == maximum_entries {
                    reverse_sort(&mut matches.0.records, accounting)?;
                    let page_bytes = workspace_vec_bytes(&page.0.entries)
                        + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
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
                let found = bounded_search(ids, accounting, |id, _| Ok(id.cmp(&entry.record.file_id)))?.1;
                let is_directory = entry.record.kind == FileKind::Directory;
                if !found && !is_directory { continue; }
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name, limits, accounting, cancellation,
                )?;
                let path = (path, allocation);
                if found && is_directory {
                    let (copy, allocation) = path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1,
                        (copy, allocation), entry.record, accounting)?;
                } else if found {
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, entry.record, accounting)?;
                    continue;
                }
                reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
                pending.0.push(path);
            }
            let next = if page.0.has_more {
                let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
                let allocation = crate::kernel::split_allocation(&mut page.1, name.retained_bytes() as u64)?;
                Some((name, allocation))
            } else { None };
            let page_bytes = workspace_vec_bytes(&page.0.entries)
                + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
            reverse_drop(page, page_bytes, accounting)?;
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                reverse_drop(old, bytes, accounting)?;
            }
            after = next;
            if after.is_none() { break; }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, accounting)?;
    let (bytes, allocation) = crate::path_index::encode_records(
        generation, ids, &matches.0.records, accounting,
    )?;
    let bytes = (bytes, allocation);
    if let Some(payload) = &bytes.0 {
        volume.fs.path_index().store(cache_key, payload, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    matches.0.complete = true;
    Ok(matches)
}

/// Reverse aliases of the actual supplied root, retaining their original allocation.
pub(crate) async fn reverse_paths_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot, generation_root: crate::ObjectId,
    file_id: FileId, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(Vec<NamespacePath>, KernelOwnedAllocation), WorkspaceError> {
    let mut search = namespace_records_to_file_root_with_accounting(
        volume, root, generation_root, &[file_id], maximum_entries, accounting, cancellation,
    ).await?;
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
    workspace_vec_bytes(records) + records.iter().map(|(_, records)|
        workspace_vec_bytes(records) + records.iter().map(|(path, _)| path.retained_bytes()).sum::<u64>()
    ).sum::<u64>()
}


impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Finds paths for one stable file identity under supplied work limits.
    pub async fn paths_for_file_id(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let search = self.namespace_records_with_accounting(
                &[file_id], maximum_entries, &mut accounting, cancellation,
            ).await?;
            let mut paths = crate::kernel::Owned::allocate(0, &mut accounting)?;
            if let Some(records) = bounded_search(&search.0.records, &mut accounting, |record, _| Ok(record.0.cmp(&file_id))).map(|(index, present)| present.then(|| &search.0.records[index].1))? {
                for (path, _) in records {
                    paths.ensure_for_push(records.len(), &mut accounting)?;
                    let (text, mut allocation) = path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                    paths.allocation.absorb(&mut allocation)?;
                    paths.bytes = paths.bytes.checked_add(text.capacity() as u64).ok_or(crate::WorkError::Overflow)?;
                    paths.value.push(text);
                }
            }
            Ok(paths.value)
        }.await;
        crate::facade::measured(result, &accounting)
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        self.namespace_records_for_file_ids(file_ids, maximum_entries).await?
            .records.into_iter().map(|(file_id, records)| {
                records.into_iter().map(|(path, _)| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>().map(|paths| (file_id, paths))
            }).collect()
    }

    pub(super) async fn namespace_records_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<NamespaceRecordSearch, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        self.namespace_records_with_accounting(
            &ids.0, maximum_entries, &mut accounting, &cancellation,
        ).await.map(|owned| owned.0)
    }

    pub(super) async fn namespace_records_with_accounting(
        &self, ids: &[FileId], maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if ids.is_empty() || maximum_entries == 0 {
            return Ok((NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() },
                accounting.claim_owned(0, 0)?));
        }
        let checkout = self.workspace.open_checkout(
            GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await?;
        let generation_root = crate::ObjectId { kind: crate::ObjectKind::GenerationRoot, digest: self.id.digest() };
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume, checkout.value.root(), generation_root,
            ids, maximum_entries, accounting, cancellation,
        ).await;
        checkout.retire(accounting)?;
        result.map_err(WorkspaceError::from)
    }
}
