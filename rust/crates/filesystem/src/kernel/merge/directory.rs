#[cfg(test)]
pub(crate) struct DirectoryMergeResult {
    pub(crate) record: Option<FileRecord>,
    pub(crate) conflicts: Vec<MergeConflict>,
    pub(crate) truncated: bool,
    pub(crate) examined_changes: u32,
}

pub(crate) struct FileFold {
    directory_id: FileId,
    name: LogicalName,
    ours: FileId,
    theirs: FileId,
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn merge_directory_record_async<S: AsyncObjectStore>(
    store: &S,
    directory_id: FileId,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    maximum_changes: u32,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<DirectoryMergeResult>, OperationFailure<MergeGenerationError>> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let mut state = MergeState::new(maximum_changes, &mut accounting)?;
        let record = merge_directory_record_with_resolutions_async(
            store,
            directory_id,
            base,
            ours,
            theirs,
            ResolutionView::Map(&BTreeMap::new()),
            maximum_conflicts,
            limits,
            &mut state,
            &mut accounting,
            cancellation,
        )
        .await?;
        Ok(DirectoryMergeResult {
            record,
            conflicts: state.conflicts.value,
            truncated: state.truncated,
            examined_changes: maximum_changes - state.remaining_changes,
        })
    }
    .await;
    let work = accounting.spent();
    result
        .map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(project_merge_error(error), work))
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
async fn merge_directory_record_with_resolutions_async<S: AsyncObjectStore>(
    store: &S,
    directory_id: FileId,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    requested: ResolutionView<'_>,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(base_entries), Some(ours_entries), Some(theirs_entries)) = (
        directory_entries(Some(base)),
        directory_entries(Some(ours)),
        directory_entries(Some(theirs)),
    ) else {
        return Err(FsError::InvalidDiff);
    };
    let supplied = match requested.file(directory_id, accounting)? {
        Some(MergeConflictResolution::Select(side)) => Some(select_value(
            *side,
            &base.metadata,
            &ours.metadata,
            &theirs.metadata,
        )),
        Some(MergeConflictResolution::File(Some(record))) if record.file_id == directory_id => {
            Some(record.metadata)
        }
        _ => None,
    };
    let metadata = if let Some(metadata) = supplied {
        Some(metadata)
    } else {
        merge_metadata_async(
            store,
            Some(base.metadata),
            ours.metadata,
            theirs.metadata,
            limits,
            accounting,
            cancellation,
        )
        .await?
    };
    let link_count = resolve_three(&base.link_count, &ours.link_count, &theirs.link_count);
    let (Some(metadata), Some(link_count)) = (metadata, link_count) else {
        state.conflict(
            MergeConflict::File(directory_id),
            maximum_conflicts,
            accounting,
        )?;
        return Ok(None);
    };
    let maximum = state.remaining_changes.max(1);
    let (ours_diff, allocation) = diff_tree_entries_with_accounting(
        store,
        Some(base_entries),
        Some(ours_entries),
        maximum,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let bytes = ours_diff.retained_allocation_bytes;
    let ours_diff = Owned::new(ours_diff, allocation, bytes);
    let result = async {
        let (theirs_diff, allocation) = diff_tree_entries_with_accounting(
            store,
            Some(base_entries),
            Some(theirs_entries),
            maximum,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let bytes = theirs_diff.retained_allocation_bytes;
        let theirs_diff = Owned::new(theirs_diff, allocation, bytes);
        let mut mutations = Owned::allocate(0, accounting)?;
        let result = async {
            if ours_diff.value.truncated || theirs_diff.value.truncated {
                return Err(FsError::MergeChangeLimit);
            }
            let mut indices = [0, 0];
            let mut conflicted = false;
            while let Some([left, right]) = next_changes(
                &ours_diff.value.changes,
                &theirs_diff.value.changes,
                &mut indices,
            ) {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                state.consume(1)?;
                let name = &left.or(right).ok_or(FsError::InvalidDiff)?.key;
                let base_value = left.or(right).and_then(|change| change.before.as_ref());
                let ours_value = left.map_or(base_value, |change| change.after.as_ref());
                let theirs_value = right.map_or(base_value, |change| change.after.as_ref());
                // Resolve borrowed entries; clone only actual retained outputs.
                let mut resolved = if ours_value == theirs_value || theirs_value == base_value {
                    Some(ours_value)
                } else if ours_value == base_value {
                    Some(theirs_value)
                } else {
                    None
                };
                // A file-level delete/modify choice can retain this binding
                // even when its isolated three-way name delta chose deletion,
                // and a file-level choice can rebind a name that one side
                // pointed at a replacement identity.
                if (resolved.is_none()
                    || (base_value.is_some() && (ours_value.is_none() ^ theirs_value.is_none()))
                    || (base_value.is_some()
                        && ours_value
                            .zip(theirs_value)
                            .is_some_and(|(ours, theirs)| ours.file_id != theirs.file_id)))
                    && let Some(resolution) = requested.binding(directory_id, name, accounting)?
                {
                    resolved = match resolution {
                        MergeConflictResolution::Select(side) => Some(match side {
                            MergeConflictSide::Base => base_value,
                            MergeConflictSide::Ours => ours_value,
                            MergeConflictSide::Theirs => theirs_value,
                        }),
                        MergeConflictResolution::Binding(id) => {
                            [base_value, ours_value, theirs_value]
                                .into_iter()
                                .find(|entry| entry.map(|entry| entry.file_id) == *id)
                        }
                        MergeConflictResolution::File(_) => None,
                    };
                }
                if resolved.is_none()
                    && requested.converges_additions()
                    && let (None, Some(a), Some(b)) = (base_value, ours_value, theirs_value)
                    && a.file_id != b.file_id
                {
                    if a.kind == super::FileKind::Directory && b.kind == super::FileKind::Directory
                    {
                        state.folds.ensure_for_push(usize::MAX, accounting)?;
                        state.folds.value.push((a.file_id, b.file_id));
                        resolved = Some(ours_value);
                    } else if a.kind != super::FileKind::Directory
                        && b.kind != super::FileKind::Directory
                    {
                        state.file_folds.ensure_for_push(usize::MAX, accounting)?;
                        let (name, mut allocation) = super::persistent_io::clone_key::<TreeFormat>(
                            name,
                            limits.maximum_name_bytes,
                            accounting,
                            None,
                            0,
                        )?;
                        let bytes = TreeFormat::key_owned_bytes(&name);
                        state.file_folds.value.push(FileFold {
                            directory_id,
                            name,
                            ours: a.file_id,
                            theirs: b.file_id,
                        });
                        state.file_folds.allocation.absorb(&mut allocation)?;
                        state.file_folds.bytes = state
                            .file_folds
                            .bytes
                            .checked_add(bytes)
                            .ok_or(WorkError::Overflow)?;
                        resolved = Some(ours_value);
                    }
                }
                let Some(resolved) = resolved else {
                    conflicted = true;
                    if state.conflicts.value.len() < maximum_conflicts as usize {
                        state
                            .conflicts
                            .ensure_for_push(maximum_conflicts as usize, accounting)?;
                        let (name, mut allocation) = super::persistent_io::clone_key::<TreeFormat>(
                            name,
                            limits.maximum_name_bytes,
                            accounting,
                            None,
                            0,
                        )?;
                        let bytes = TreeFormat::key_owned_bytes(&name);
                        state
                            .conflicts
                            .value
                            .push(MergeConflict::Binding { directory_id, name });
                        state.conflicts.allocation.absorb(&mut allocation)?;
                        state.conflicts.bytes = state
                            .conflicts
                            .bytes
                            .checked_add(bytes)
                            .ok_or(WorkError::Overflow)?;
                    } else {
                        state.truncated = true;
                    }
                    continue;
                };
                if resolved != ours_value {
                    mutations.ensure_for_push(usize::MAX, accounting)?;
                    let (mutation, mut allocation, bytes) = if let Some(entry) = resolved {
                        let (entry, allocation) = super::persistent_io::clone_value::<TreeFormat>(
                            entry,
                            limits.maximum_name_bytes,
                            accounting,
                            None,
                            0,
                        )?;
                        let bytes = TreeFormat::value_owned_bytes(&entry);
                        let mutation = if let Some(before) = ours_value {
                            TreeMutation::Replace {
                                entry,
                                expected_file_id: before.file_id,
                            }
                        } else {
                            TreeMutation::Insert(entry)
                        };
                        (mutation, allocation, bytes)
                    } else {
                        let (name, allocation) = super::persistent_io::clone_key::<TreeFormat>(
                            name,
                            limits.maximum_name_bytes,
                            accounting,
                            None,
                            0,
                        )?;
                        let bytes = TreeFormat::key_owned_bytes(&name);
                        (
                            TreeMutation::Remove {
                                name,
                                expected_file_id: ours_value.map(|entry| entry.file_id),
                            },
                            allocation,
                            bytes,
                        )
                    };
                    mutations.value.push(mutation);
                    mutations.allocation.absorb(&mut allocation)?;
                    mutations.bytes = mutations
                        .bytes
                        .checked_add(bytes)
                        .ok_or(WorkError::Overflow)?;
                }
            }
            if conflicted {
                return Ok(None);
            }
            let entries = if mutations.value.is_empty() {
                ours_entries
            } else {
                apply_tree_mutations_with_accounting(
                    store,
                    ours_entries,
                    &mutations.value,
                    maximum,
                    limits,
                    accounting,
                    cancellation,
                )
                .await?
                .value
            };
            Ok(Some(FileRecord {
                file_id: directory_id,
                kind: super::FileKind::Directory,
                link_count,
                metadata,
                payload: FilePayload::Directory { entries },
            }))
        }
        .await;
        mutations.retire(accounting)?;
        theirs_diff.retire(accounting)?;
        result
    }
    .await;
    ours_diff.retire(accounting)?;
    result
}
