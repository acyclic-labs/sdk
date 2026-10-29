async fn fold_additions<S: AsyncObjectStore>(
    store: &S,
    requested: ResolutionView<'_>,
    maximum_conflicts: impl Into<Option<u32>>,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let maximum_conflicts = maximum_conflicts.into();

    while let Some((ours_id, theirs_id)) = state.folds.value.pop() {
        let (ours_before, ours) = state.record(ours_id, accounting)?;
        let (theirs_before, theirs) = state.record(theirs_id, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else {
            return Err(FsError::InvalidDiff);
        };
        let empty = FileRecord {
            payload: FilePayload::Directory {
                entries: empty_tree_async(store, limits, accounting, cancellation).await?,
            },
            ..ours
        };
        let record = merge_directory_record_with_resolutions_async(
            store,
            ours_id,
            empty,
            ours,
            theirs,
            requested,
            maximum_conflicts,
            limits,
            state,
            accounting,
            cancellation,
        )
        .await?;
        if let Some(record) = record {
            state.replace(ours_id, (ours_before, Some(record)), accounting)?;
        }
        state.replace(theirs_id, (theirs_before, None), accounting)?;
    }
    state.file_folds.value.reverse();
    while let Some(fold) = state.file_folds.value.pop() {
        let (ours_before, ours) = state.record(fold.ours, accounting)?;
        let (theirs_before, theirs) = state.record(fold.theirs, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else {
            return Err(FsError::InvalidDiff);
        };
        let metadata = if ours.kind == theirs.kind
            && ours.payload == theirs.payload
            && ours.link_count == 1
            && theirs.link_count == 1
        {
            converge_metadata_async(
                store,
                ours.metadata,
                theirs.metadata,
                limits,
                accounting,
                cancellation,
            )
            .await?
        } else {
            None
        };
        let bytes = TreeFormat::key_owned_bytes(&fold.name);
        let allocation =
            super::persistent_point::split_allocation(&mut state.file_folds.allocation, bytes)?;
        state.file_folds.bytes = state
            .file_folds
            .bytes
            .checked_sub(bytes)
            .ok_or(WorkError::Overflow)?;
        let name = Owned::new(fold.name, allocation, bytes);
        if let Some(metadata) = metadata {
            state.replace(
                fold.ours,
                (ours_before, Some(FileRecord { metadata, ..ours })),
                accounting,
            )?;
            state.replace(fold.theirs, (theirs_before, None), accounting)?;
            name.retire(accounting)?;
        } else if state.conflicts.value.len()
            < maximum_conflicts.map_or(usize::MAX, |maximum| maximum as usize)
        {
            state.conflicts.ensure_for_push(
                maximum_conflicts.map_or(usize::MAX, |maximum| maximum as usize),
                accounting,
            )?;
            let Owned {
                value: name,
                mut allocation,
                bytes,
            } = name;
            state.conflicts.value.push(MergeConflict::Binding {
                directory_id: fold.directory_id,
                name,
            });
            state.conflicts.allocation.absorb(&mut allocation)?;
            state.conflicts.bytes = state
                .conflicts
                .bytes
                .checked_add(bytes)
                .ok_or(WorkError::Overflow)?;
        } else {
            state.truncated = true;
            name.retire(accounting)?;
        }
    }
    Ok(())
}

struct DirectoryTransition {
    directory: FileId,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    contribution: Owned<Vec<(FileId, i64)>>,
    diff: Owned<super::persistent_diff::Diff<LogicalName, super::TreeEntry>>,
}

impl DirectoryTransition {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let diff = self.diff.retire(accounting);
        let contribution = self.contribution.retire(accounting);
        diff?;
        contribution
    }
}

fn add_delta(
    deltas: &mut Owned<Vec<(FileId, i64)>>,
    file: FileId,
    delta: i64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    match super::search::binary_search_with_accounting(&deltas.value, accounting, |(id, _)| {
        id.cmp(&file)
    })? {
        Ok(index) => {
            let value = &mut deltas.value.get_mut(index).ok_or(FsError::InvalidDiff)?.1;
            *value = value.checked_add(delta).ok_or(WorkError::Overflow)?;
        }
        Err(index) => {
            deltas.ensure_for_push(usize::MAX, accounting)?;
            deltas.value.insert(index, (file, delta));
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Link-count validation and edits share the admitted mutation frame."
)]
async fn adjust_link_counts<S: AsyncObjectStore>(
    store: &S,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<Vec<DirectoryTransition>>, FsError> {
    let mut transitions: Owned<Vec<DirectoryTransition>> = Owned::allocate(0, accounting)?;
    let result = async {
        loop {
            accounting.check()?;
            let mut deltas = Owned::allocate(0, accounting)?;
            let result = async {
                for (directory, (before, after)) in &state.records.value {
                    let before = directory_entries(*before);
                    let after = directory_entries(*after);
                    let cached = super::search::binary_search_with_accounting(
                        &transitions.value,
                        accounting,
                        |entry| entry.directory.cmp(directory),
                    )?;
                    if before == after {
                        if let Ok(index) = cached {
                            transitions.value.remove(index).retire(accounting)?;
                        }
                        continue;
                    }
                    let unchanged = cached
                        .ok()
                        .and_then(|index| transitions.value.get(index))
                        .is_some_and(|entry| entry.before == before && entry.after == after);
                    if !unchanged {
                        if state.changes.remaining == Some(0) {
                            return Err(FsError::MergeChangeLimit);
                        }
                        let (diff, allocation) = diff_tree_entries_with_accounting(
                            store,
                            before,
                            after,
                            state.changes.remaining,
                            limits,
                            accounting,
                            cancellation,
                        )
                        .await?;
                        let bytes = diff.retained_allocation_bytes;
                        let diff = Owned::new(diff, allocation, bytes);
                        let mut contribution = Owned::allocate(0, accounting)?;
                        let result = (|| {
                            if diff.value.truncated {
                                return Err(FsError::MergeChangeLimit);
                            }
                            for change in &diff.value.changes {
                                if let Some(before) = &change.before {
                                    add_delta(&mut contribution, before.file_id, -1, accounting)?;
                                }
                                if let Some(after) = &change.after {
                                    add_delta(&mut contribution, after.file_id, 1, accounting)?;
                                }
                            }
                            Ok(())
                        })();
                        let count = diff.value.changes.len();
                        if let Err(error) = result {
                            let retired_diff = diff.retire(accounting);
                            let retired_contribution = contribution.retire(accounting);
                            retired_diff?;
                            retired_contribution?;
                            return Err(error);
                        }
                        state.changes.consume(count)?;
                        let entry = DirectoryTransition {
                            directory: *directory,
                            before,
                            after,
                            contribution,
                            diff,
                        };
                        match cached {
                            Ok(index) => {
                                let previous = std::mem::replace(
                                    transitions
                                        .value
                                        .get_mut(index)
                                        .ok_or(FsError::InvalidDiff)?,
                                    entry,
                                );
                                previous.retire(accounting)?;
                            }
                            Err(index) => {
                                transitions.ensure_for_push(usize::MAX, accounting)?;
                                transitions.value.insert(index, entry);
                            }
                        }
                    }
                    let index = super::search::binary_search_with_accounting(
                        &transitions.value,
                        accounting,
                        |entry| entry.directory.cmp(directory),
                    )?
                    .map_err(|_| FsError::InvalidDiff)?;
                    for (file, delta) in &transitions
                        .value
                        .get(index)
                        .ok_or(FsError::InvalidDiff)?
                        .contribution
                        .value
                    {
                        add_delta(&mut deltas, *file, *delta, accounting)?;
                    }
                }
                for (file, delta) in &deltas.value {
                    if *delta != 0 {
                        state.record(*file, accounting)?;
                    }
                }
                let mut pruned = false;
                for (file, (ours, resolved)) in &mut state.records.value {
                    let delta = super::search::binary_search_with_accounting(
                        &deltas.value,
                        accounting,
                        |(id, _)| id.cmp(file),
                    )?
                    .ok()
                    .and_then(|index| deltas.value.get(index))
                    .map_or(0, |(_, delta)| *delta);
                    let links = i128::from(ours.map_or(0, |record| record.link_count))
                        .checked_add(i128::from(delta))
                        .and_then(|value| u64::try_from(value).ok())
                        .ok_or(FsError::InvalidDiff)?;
                    if resolved.is_none() {
                        if links != 0 {
                            return Err(FsError::InvalidDiff);
                        }
                    } else if links == 0 {
                        pruned |= is_directory(*resolved);
                        *resolved = None;
                    } else if !pruned && let Some(record) = resolved {
                        record.link_count = links;
                    }
                }
                Ok(pruned)
            }
            .await;
            deltas.retire(accounting)?;
            if !result? {
                return Ok(());
            }
        }
    }
    .await;
    if let Err(error) = result {
        retire_transitions(transitions, accounting)?;
        return Err(error);
    }
    Ok(transitions)
}

fn retire_transitions(
    mut transitions: Owned<Vec<DirectoryTransition>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let mut failure = None;
    for transition in transitions.value.drain(..) {
        if let Err(error) = transition.retire(accounting) {
            failure.get_or_insert(error);
        }
    }
    let retired = transitions.retire(accounting);
    if let Some(error) = failure {
        return Err(error);
    }
    retired
}

async fn rewrite_merge_bindings<S: AsyncObjectStore>(
    store: &S,
    root_file_id: FileId,
    limits: DecodeLimits,
    state: &mut MergeState,
    transitions: &[DirectoryTransition],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let mut edits = Owned::<Vec<super::bindings::FileBindingEdit>>::allocate(0, accounting)?;
    let result = async {
        for (_, (before, after)) in &mut state.records.value {
            if let Some(after) = after {
                after.bindings = before.and_then(|record| record.bindings);
            }
        }
        for transition in transitions {
            for (order, change) in transition.diff.value.changes.iter().enumerate() {
                let order = u32::try_from(order).map_err(|_| WorkError::Overflow)?;
                for (file, present) in change
                    .before
                    .as_ref()
                    .map(|entry| (entry.file_id, false))
                    .into_iter()
                    .chain(change.after.as_ref().map(|entry| (entry.file_id, true)))
                {
                    let (before, after) = state.record(file, accounting)?;
                    if after.is_none() {
                        continue;
                    }
                    if file == root_file_id
                        || before.is_some_and(|record| record.bindings.is_none())
                    {
                        return Err(FsError::InvalidDiff);
                    }
                    super::bindings::push_edit(
                        &mut edits,
                        file,
                        order,
                        transition.directory,
                        &change.key,
                        present,
                        limits.maximum_name_bytes,
                        accounting,
                    )?;
                }
            }
        }
        super::bindings::sort_edits(&mut edits, accounting)?;
        for group in edits
            .value
            .chunk_by(|left, right| left.file_id == right.file_id)
        {
            let file = group.first().ok_or(FsError::InvalidDiff)?.file_id;
            let (before, after) = state.record(file, accounting)?;
            let mut after = after.ok_or(FsError::InvalidDiff)?;
            let root = super::bindings::apply_with_accounting(
                store,
                before.and_then(|record| record.bindings),
                group,
                u32::try_from(group.len()).map_err(|_| WorkError::Overflow)?,
                limits,
                accounting,
                cancellation,
            )
            .await?;
            after.bindings = Some(root);
            state.replace(file, (before, Some(after)), accounting)?;
        }
        Ok(())
    }
    .await;
    edits.retire(accounting)?;
    result
}
