//! New bindings carry their own names; only existing parent paths need lookup.
use super::*;

type Paths = (Vec<ChangedPath>, KernelOwnedAllocation);

pub(super) async fn created_paths(
    endpoints: &impl NamespaceEndpoints,
    changes: &GenerationDiff,
    maximum_entries: u32,
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Paths>, WorkspaceError> {
    if !only_creates(changes, accounting)? {
        return Ok(None);
    }
    // Fund the anchor scan before examining any change pairs.
    accounting.charge_items(
        (changes.bindings.len() as u64)
            .checked_mul(changes.files.len() as u64)
            .ok_or(crate::WorkError::Overflow)?,
    )?;
    let anchors = sorted_changed_file_ids(
        changes.bindings.iter().filter_map(|binding| {
            let new_parent = changes
                .files
                .iter()
                .any(|file| file.file_id == binding.directory_id && file.before.is_none());
            (!new_parent).then_some(binding.directory_id)
        }),
        accounting,
    )?;
    let parents = endpoints
        .search(false, &anchors.0, maximum_entries, accounting, cancellation)
        .await?;
    if !parents.0.complete {
        return Err(WorkspaceError::ChangedPathLimit);
    }
    let result = build_paths(
        changes,
        &parents.0.records,
        limits,
        accounting,
        cancellation,
    );
    let bytes = namespace_records_bytes(&parents.0.records);
    reverse_drop(parents, bytes, accounting)?;
    let bytes = workspace_vec_bytes(&anchors.0);
    reverse_drop(anchors, bytes, accounting)?;
    result.map(Some)
}

fn only_creates(
    changes: &GenerationDiff,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, WorkspaceError> {
    if changes.bindings.is_empty() {
        return Ok(false);
    }
    for file in &changes.files {
        accounting.charge_items(1)?;
        let accepted = match (&file.before, &file.after) {
            (Some(before), Some(after)) => !record_changed(before, after),
            (None, Some(after)) => after.link_count == 1,
            _ => false,
        };
        if !accepted {
            return Ok(false);
        }
    }
    for binding in &changes.bindings {
        accounting.charge_items(1)?;
        let Some(entry) = binding.after.as_ref().filter(|_| binding.before.is_none()) else {
            return Ok(false);
        };
        let Some(file) = changed_file(changes, entry.file_id, accounting)? else {
            return Ok(false);
        };
        if file.before.is_some() || file.after.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn build_paths(
    changes: &GenerationDiff,
    parents: &NamespaceRecords,
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Paths, WorkspaceError> {
    let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
    while paths.0.len() < changes.bindings.len() {
        let before = paths.0.len();
        for binding in &changes.bindings {
            append_binding(
                &mut paths,
                parents,
                binding,
                changes,
                limits,
                accounting,
                cancellation,
            )?;
        }
        if paths.0.len() == before {
            return Err(FsError::InvalidDiff.into());
        }
    }
    Ok(paths)
}

fn append_binding(
    paths: &mut Paths,
    parents: &NamespaceRecords,
    binding: &crate::facade::DirectoryBindingChange,
    changes: &GenerationDiff,
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    // At most two output scans and one parent scan precede path construction.
    accounting.charge_items((2 * paths.0.len() + parents.len() + 1) as u64)?;
    let entry = binding.after.as_ref().ok_or(FsError::InvalidDiff)?;
    if paths.0.iter().any(|path| {
        path.after
            .is_some_and(|record| record.file_id == entry.file_id)
    }) {
        return Ok(());
    }
    let existing = parents.iter().find(|(id, _)| *id == binding.directory_id);
    let parent = if let Some((_, entries)) = existing {
        if entries.len() != 1 {
            return Err(FsError::InvalidDiff.into());
        }
        entries.first().map(|(path, _)| path)
    } else {
        paths
            .0
            .iter()
            .find(|path| {
                path.after
                    .is_some_and(|record| record.file_id == binding.directory_id)
            })
            .map(|path| &path.path)
    };
    let Some(parent) = parent else {
        return Ok(());
    };
    let path = parent.child_with_accounting(&binding.name, limits, accounting, cancellation)?;
    let record = changed_file(changes, entry.file_id, accounting)?
        .and_then(|file| file.after)
        .ok_or(FsError::InvalidDiff)?;
    merge_changed_record(
        paths,
        &path.0,
        Some(record),
        false,
        accounting,
        cancellation,
    )?;
    let bytes = path.0.retained_bytes();
    reverse_drop(path, bytes, accounting)?;
    Ok(())
}

fn changed_file<'a>(
    changes: &'a GenerationDiff,
    id: FileId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<&'a crate::facade::FileRecordChange>, WorkspaceError> {
    let position =
        binary_search_with_accounting(&changes.files, accounting, |file| file.file_id.cmp(&id))?;
    Ok(position.ok().and_then(|index| changes.files.get(index)))
}
