// Identity follows the name when one side replaced a file.
//
// Writers that replace a file (atomic saves, checkouts that rewrite files,
// writes through a mount that create the file anew) delete the old identity
// and bind the same name to a new one. Matched by identity alone, that reads
// as a delete/modify of the old identity plus an unrelated addition, so even
// disjoint or equal edits conflict. A delete/modify of a single-link regular
// file whose deleting side bound the same name to a new single-link regular
// file is therefore merged as one file: the replacement is the deleting side's
// value of the base identity, the name is bound back to the base identity,
// and the replacement identity is dropped.

fn single_regular(record: Option<FileRecord>) -> bool {
    record.is_some_and(|record| record.kind == super::FileKind::Regular && record.link_count == 1)
}

/// A delete/modify of a single-link regular file, which a replacement on the
/// deleting side can turn into an ordinary three-way file merge.
fn replaceable([base, ours, theirs]: [Option<FileRecord>; 3]) -> bool {
    single_regular(base)
        && ours.is_none() != theirs.is_none()
        && (single_regular(ours) || single_regular(theirs))
}

/// Settles a conflicting record: extents, then fields, then a supplied
/// resolution. `None` means it stays conflicted.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn settle_record<S: AsyncObjectStore>(
    store: &S,
    file: FileId,
    [base, ours, theirs]: [Option<FileRecord>; 3],
    mut resolved: OptionalResolution,
    resolutions: ResolutionView<'_>,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Option<FileRecord>>, FsError> {
    if matches!(resolved, OptionalResolution::Conflict)
        && is_extent_regular(base)
        && is_extent_regular(ours)
        && is_extent_regular(theirs)
        && let Some((record, examined)) = merge_regular_record_async(
            store,
            base.ok_or(FsError::InvalidDiff)?,
            ours.ok_or(FsError::InvalidDiff)?,
            theirs.ok_or(FsError::InvalidDiff)?,
            state.remaining_changes,
            limits,
            accounting,
            cancellation,
        )
        .await?
    {
        state.consume(examined as usize)?;
        resolved = OptionalResolution::Resolved(Some(record));
    }
    if matches!(resolved, OptionalResolution::Conflict) {
        resolved = merge_records_async(
            store,
            [base, ours, theirs],
            limits,
            accounting,
            cancellation,
        )
        .await?
        .map_or(OptionalResolution::Conflict, |record| {
            OptionalResolution::Resolved(Some(record))
        });
    }
    Ok(match resolved {
        OptionalResolution::Resolved(record) => Some(record),
        OptionalResolution::Conflict => match resolutions.file(file, accounting)? {
            Some(MergeConflictResolution::Select(side)) => {
                Some(select_optional(*side, base, ours, theirs))
            }
            Some(MergeConflictResolution::File(record)) => {
                if record.is_some_and(|record| record.file_id != file) {
                    return Err(FsError::InvalidDiff);
                }
                Some(*record)
            }
            Some(MergeConflictResolution::Binding(_)) => return Err(FsError::InvalidDiff),
            None => None,
        },
    })
}

/// A name one side rebound from a base identity to a file it created.
struct Replacement {
    /// The base identity the name was bound to.
    replaced: FileId,
    directory: FileId,
    name: LogicalName,
    /// The created single-link regular file now bound to the name.
    record: FileRecord,
}

/// Every name one side's changed directories rebound from a deferred
/// identity to a single-link regular file that side created, sorted by the
/// replaced identity. Each changed directory is diffed and charged once, no
/// matter how many deferred files it holds.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn replacement_index<S: AsyncObjectStore>(
    store: &S,
    changes: &[super::persistent_diff::ValueChange<FileId, FileRecord>],
    side: usize,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<Vec<Replacement>>, FsError> {
    let mut index: Owned<Vec<Replacement>> = Owned::allocate(0, accounting)?;
    // A side that deleted no deferred identity cannot hold a replacement;
    // its changed directories are neither read nor charged.
    if !state
        .replacements
        .value
        .iter()
        .any(|(_, values)| deleting_side(values) == side)
    {
        return Ok(index);
    }
    let built = async {
        for change in changes {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let (Some(before), Some(after)) = (
                directory_entries(change.before),
                directory_entries(change.after),
            ) else {
                continue;
            };
            if before == after {
                continue;
            }
            if state.remaining_changes == 0 {
                return Err(FsError::MergeChangeLimit);
            }
            let (diff, allocation) = diff_tree_entries_with_accounting(
                store,
                Some(before),
                Some(after),
                state.remaining_changes,
                limits,
                accounting,
                cancellation,
            )
            .await?;
            let bytes = diff.retained_allocation_bytes;
            let diff = Owned::new(diff, allocation, bytes);
            let found = (|| {
                if diff.value.truncated {
                    return Err(FsError::MergeChangeLimit);
                }
                state.consume(diff.value.changes.len())?;
                for entry in &diff.value.changes {
                    let Some((replaced, record)) =
                        rebound_record(entry, changes, side, &state.replacements.value, accounting)?
                    else {
                        continue;
                    };
                    push_replacement(
                        &mut index,
                        replaced,
                        change.key,
                        &entry.key,
                        record,
                        limits.maximum_name_bytes,
                        accounting,
                    )?;
                }
                Ok(())
            })();
            diff.retire(accounting)?;
            found?;
        }
        index
            .value
            .sort_unstable_by_key(|replacement| replacement.replaced);
        Ok(())
    }
    .await;
    match built {
        Ok(()) => Ok(index),
        Err(error) => {
            index.retire(accounting)?;
            Err(error)
        }
    }
}

/// The base identity `entry` rebinds and the record `side` rebound its name
/// to, when `side` deleted that deferred identity and created, as a
/// single-link regular file, the identity now under the name.
fn rebound_record(
    entry: &super::persistent_diff::ValueChange<LogicalName, super::TreeEntry>,
    changes: &[super::persistent_diff::ValueChange<FileId, FileRecord>],
    side: usize,
    deferred: &[(FileId, [Option<FileRecord>; 3])],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(FileId, FileRecord)>, FsError> {
    let (Some(old), Some(new)) = (&entry.before, &entry.after) else {
        return Ok(None);
    };
    if old.file_id == new.file_id || new.kind != super::FileKind::Regular {
        return Ok(None);
    }
    // Only identities deferred with this side as the deleter.
    let deleted_here =
        super::search::binary_search_with_accounting(deferred, accounting, |(file, _)| {
            file.cmp(&old.file_id)
        })?
        .ok()
        .and_then(|position| deferred.get(position))
        .is_some_and(|(_, values)| deleting_side(values) == side);
    if !deleted_here {
        return Ok(None);
    }
    // The new identity must be one this side created.
    let record = super::search::binary_search_with_accounting(changes, accounting, |change| {
        change.key.cmp(&new.file_id)
    })?
    .ok()
    .and_then(|position| changes.get(position))
    .filter(|change| change.before.is_none())
    .and_then(|change| change.after)
    .filter(|record| single_regular(Some(*record)));
    Ok(record.map(|record| (old.file_id, record)))
}

/// Appends one replacement to `index`, owning a charged copy of its name.
fn push_replacement(
    index: &mut Owned<Vec<Replacement>>,
    replaced: FileId,
    directory: FileId,
    key: &LogicalName,
    record: FileRecord,
    maximum_name_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let (name, mut allocation) =
        super::persistent_io::clone_key::<TreeFormat>(key, maximum_name_bytes, accounting, None, 0)?;
    let bytes = TreeFormat::key_owned_bytes(&name);
    if let Err(error) = index.ensure_for_push(usize::MAX, accounting) {
        Owned::new(name, allocation, bytes).retire(accounting)?;
        return Err(error);
    }
    index.allocation.absorb(&mut allocation)?;
    index.bytes = index.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
    index.value.push(Replacement {
        replaced,
        directory,
        name,
        record,
    });
    Ok(())
}

/// The side that deleted a deferred base identity: 0 is ours, 1 is theirs.
fn deleting_side([_, ours, _]: &[Option<FileRecord>; 3]) -> usize {
    usize::from(ours.is_some())
}

/// The replacement `index` records for `file`, with an owned copy of its name.
fn find_replacement(
    index: &[Replacement],
    file: FileId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(FileId, Owned<LogicalName>, FileRecord)>, FsError> {
    let Some(replacement) =
        super::search::binary_search_with_accounting(index, accounting, |replacement| {
            replacement.replaced.cmp(&file)
        })?
        .ok()
        .and_then(|position| index.get(position))
    else {
        return Ok(None);
    };
    let (name, allocation) = super::persistent_io::clone_key::<TreeFormat>(
        &replacement.name,
        limits.maximum_name_bytes,
        accounting,
        None,
        0,
    )?;
    let bytes = TreeFormat::key_owned_bytes(&name);
    Ok(Some((
        replacement.directory,
        Owned::new(name, allocation, bytes),
        replacement.record,
    )))
}

/// Whether the merged directory binds `name` to `file`.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn merged_binding<S: AsyncObjectStore>(
    store: &S,
    directory: FileId,
    name: &LogicalName,
    file: FileId,
    limits: DecodeLimits,
    state: &MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let (_, after) = state.record(directory, accounting)?;
    let Some(entries) = directory_entries(after) else {
        return Ok(false);
    };
    let (entry, allocation) = super::read::lookup_tree_entry_async_with_accounting(
        store,
        entries,
        name,
        limits,
        accounting,
        cancellation,
        None,
    )
    .await?;
    let bound = entry.as_ref().is_some_and(|entry| entry.file_id == file);
    let retained = entry
        .as_ref()
        .map_or(0, |entry| entry.name.retained_bytes() as u64);
    Owned::new(entry, allocation, retained).retire(accounting)?;
    Ok(bound)
}

/// Binds `name` in the merged directory back to `target`, or removes it when
/// the merged file was deleted.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn rebind_replacement<S: AsyncObjectStore>(
    store: &S,
    directory: FileId,
    name: Owned<LogicalName>,
    replacement: FileId,
    target: Option<FileId>,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let (before, after) = state.record(directory, accounting)?;
    let (Some(record), Some(entries)) = (after, directory_entries(after)) else {
        name.retire(accounting)?;
        return Err(FsError::InvalidDiff);
    };
    let Owned {
        value: name,
        mut allocation,
        bytes,
    } = name;
    let mutation = match target {
        Some(file_id) => TreeMutation::Replace {
            entry: super::TreeEntry {
                name,
                file_id,
                kind: super::FileKind::Regular,
            },
            expected_file_id: replacement,
        },
        None => TreeMutation::Remove {
            name,
            expected_file_id: Some(replacement),
        },
    };
    let mut mutations = Owned::allocate(0, accounting)?;
    let result = async {
        mutations.ensure_for_push(1, accounting)?;
        mutations.value.push(mutation);
        mutations.allocation.absorb(&mut allocation)?;
        mutations.bytes = mutations
            .bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        Ok::<_, FsError>(
            apply_tree_mutations_with_accounting(
                store,
                entries,
                &mutations.value,
                state.remaining_changes.max(1),
                limits,
                accounting,
                cancellation,
            )
            .await?
            .value,
        )
    }
    .await;
    mutations.retire(accounting)?;
    let entries = result?;
    state.replace(
        directory,
        (
            before,
            Some(FileRecord {
                payload: FilePayload::Directory { entries },
                ..record
            }),
        ),
        accounting,
    )
}

/// Settles every deferred delete/modify, merging a replaced file with the
/// base identity it replaced.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn settle_replacements<S: AsyncObjectStore>(
    store: &S,
    sides: [&[super::persistent_diff::ValueChange<FileId, FileRecord>]; 2],
    resolutions: ResolutionView<'_>,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    if state.replacements.value.is_empty() {
        return Ok(());
    }
    // One index per deleting side: 0 is ours, 1 is theirs.
    let ours_index =
        replacement_index(store, sides[0], 0, limits, state, accounting, cancellation).await?;
    let theirs_index = match replacement_index(
        store,
        sides[1],
        1,
        limits,
        state,
        accounting,
        cancellation,
    )
    .await
    {
        Ok(index) => index,
        Err(error) => {
            ours_index.retire(accounting)?;
            return Err(error);
        }
    };
    let indexes = [ours_index, theirs_index];
    let settled = settle_indexed(
        store,
        &indexes,
        resolutions,
        maximum_conflicts,
        limits,
        state,
        accounting,
        cancellation,
    )
    .await;
    for index in indexes {
        index.retire(accounting)?;
    }
    settled
}

/// Settles every deferred delete/modify against the per-side replacement
/// indexes.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn settle_indexed<S: AsyncObjectStore>(
    store: &S,
    indexes: &[Owned<Vec<Replacement>>; 2],
    resolutions: ResolutionView<'_>,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let mut index = 0;
    while let Some(&(file, values)) = state.replacements.value.get(index) {
        index += 1;
        let [base, ours, theirs] = values;
        let [ours_index, theirs_index] = indexes;
        let theirs_deleted = deleting_side(&values) == 1;
        let side_index = if theirs_deleted {
            theirs_index
        } else {
            ours_index
        };
        let mut replacement = find_replacement(&side_index.value, file, limits, accounting)?;
        let unbound = if let Some((directory, name, record)) = &replacement {
            !merged_binding(
                store,
                *directory,
                &name.value,
                record.file_id,
                limits,
                state,
                accounting,
                cancellation,
            )
            .await?
        } else {
            false
        };
        if unbound && let Some((_, name, _)) = replacement.take() {
            name.retire(accounting)?;
        }
        // The replacement stands in as the deleting side's value of the base
        // identity.
        let standin = replacement.as_ref().map(|(_, _, record)| FileRecord {
            file_id: file,
            ..*record
        });
        let paired = match standin {
            Some(record) if theirs_deleted => [base, ours, Some(record)],
            Some(record) => [base, Some(record), theirs],
            None => values,
        };
        let [paired_base, paired_ours, paired_theirs] = paired;
        let resolved = resolve_optional(paired_base, paired_ours, paired_theirs);
        let settled = settle_record(
            store,
            file,
            paired,
            resolved,
            resolutions,
            limits,
            state,
            accounting,
            cancellation,
        )
        .await?;
        match (settled, replacement) {
            (Some(after), Some((directory, name, record))) => {
                state.replace(file, (ours, after), accounting)?;
                let (replacement_before, _) = state.record(record.file_id, accounting)?;
                state.replace(record.file_id, (replacement_before, None), accounting)?;
                rebind_replacement(
                    store,
                    directory,
                    name,
                    record.file_id,
                    after.map(|_| file),
                    limits,
                    state,
                    accounting,
                    cancellation,
                )
                .await?;
            }
            (settled, replacement) => {
                if let Some((_, name, _)) = replacement {
                    name.retire(accounting)?;
                }
                let after = if let Some(after) = settled {
                    after
                } else {
                    state.conflict(MergeConflict::File(file), maximum_conflicts, accounting)?;
                    ours
                };
                state.replace(file, (ours, after), accounting)?;
            }
        }
    }
    Ok(())
}
