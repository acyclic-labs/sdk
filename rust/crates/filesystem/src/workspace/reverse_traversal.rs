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
    // Keep both groups reachable by their original aggregate owner until the
    // destination is funded. A refused growth must still retire every alias.
    while !records
        .get(source)
        .ok_or(FsError::InvalidDiff)?
        .1
        .is_empty()
    {
        reverse_grow(
            &mut records.get_mut(target).ok_or(FsError::InvalidDiff)?.1,
            allocation,
            accounting,
        )?;
        accounting.charge(copy_work(
            std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>() as u64,
        ))?;
        let entry = records
            .get_mut(source)
            .ok_or(FsError::InvalidDiff)?
            .1
            .pop()
            .ok_or(FsError::InvalidDiff)?;
        records
            .get_mut(target)
            .ok_or(FsError::InvalidDiff)?
            .1
            .push(entry);
    }
    let source = std::mem::take(&mut records.get_mut(source).ok_or(FsError::InvalidDiff)?.1);
    let bytes = workspace_vec_bytes(&source);
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
    maximum_entries: impl Into<Option<u32>>,
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
    maximum_entries: impl Into<Option<u32>>,
    matches: &mut (NamespaceRecordSearch, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let maximum_entries = maximum_entries.into();
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
        let mut emitted = 0_u64;
        for record in records.value.iter().flatten().copied() {
            let mut paths = crate::kernel::direct_paths::DirectPaths::new(
                view.root,
                record,
                view.config,
                accounting,
            )?;
            let result = async {
                let mut aliases = 0_u64;
                loop {
                    // The authenticated count proves another alias remains;
                    // do not reconstruct that path after the output is full.
                    if maximum_entries.is_some_and(|maximum| emitted == u64::from(maximum))
                        && aliases < record.link_count
                    {
                        return Ok(false);
                    }
                    let Some(path) = paths.next(view.objects, accounting, cancellation).await?
                    else {
                        break;
                    };
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        (path.value, path.allocation),
                        record,
                        accounting,
                    )?;
                    emitted += 1;
                    aliases += 1;
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
