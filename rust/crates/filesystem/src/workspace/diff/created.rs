//! New bindings carry their own names; only existing parent paths need lookup.
use super::*;

type Paths = (Vec<ChangedPath>, KernelOwnedAllocation);

pub(super) async fn created_paths(
    endpoints: &impl NamespaceEndpoints,
    changes: &GenerationDiff,
    maximum_entries: Option<u32>,
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Paths>, WorkspaceError> {
    if !only_creates(changes, accounting)? {
        return Ok(None);
    }
    let mut anchors = (Vec::new(), accounting.claim_owned(0, 0)?);
    for binding in &changes.bindings {
        accounting.charge_items(1)?;
        let new_parent = changed_file(changes, binding.directory_id, accounting)?
            .is_some_and(|file| file.before.is_none());
        if !new_parent {
            reverse_grow(&mut anchors.0, &mut anchors.1, accounting)?;
            accounting.charge(copy_work(std::mem::size_of::<FileId>() as u64))?;
            anchors.0.push(binding.directory_id);
        }
    }
    let sorted = sorted_changed_file_ids(anchors.0.iter().copied(), accounting)?;
    let bytes = workspace_vec_bytes(&anchors.0);
    reverse_drop(anchors, bytes, accounting)?;
    let anchors = sorted;
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

#[derive(Clone, Copy)]
enum CreationState {
    Pending,
    Visiting,
    Complete(usize),
}

struct CreationNode {
    file_id: FileId,
    binding: usize,
    state: CreationState,
}

fn creation_index(
    nodes: &[CreationNode],
    id: FileId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<usize>, FsError> {
    binary_search_with_accounting(nodes, accounting, |node| node.file_id.cmp(&id)).map(Result::ok)
}

fn creation_state(
    nodes: &mut [CreationNode],
    index: usize,
    state: CreationState,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let copied = copy_work(std::mem::size_of::<CreationState>() as u64);
    accounting.admit(copied)?;
    nodes.get_mut(index).ok_or(FsError::InvalidDiff)?.state = state;
    accounting.record_completed(copied)
}

fn build_paths(
    changes: &GenerationDiff,
    parents: &NamespaceRecords,
    limits: crate::model::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Paths, WorkspaceError> {
    let mut nodes =
        crate::kernel::Owned::<Vec<CreationNode>>::allocate(changes.bindings.len(), accounting)?;
    for (index, binding) in changes.bindings.iter().enumerate() {
        accounting.charge_items(1)?;
        let entry = binding.after.as_ref().ok_or(FsError::InvalidDiff)?;
        accounting.charge(copy_work(std::mem::size_of::<CreationNode>() as u64))?;
        nodes.value.push(CreationNode {
            file_id: entry.file_id,
            binding: index,
            state: CreationState::Pending,
        });
    }
    crate::kernel::sort_by_with_accounting(&mut nodes.value, accounting, |left, right| {
        left.file_id.cmp(&right.file_id)
    })?;
    for pair in nodes.value.windows(2) {
        accounting.charge_items(1)?;
        if pair[0].file_id == pair[1].file_id {
            return Err(FsError::InvalidDiff.into());
        }
    }
    let mut pending = (Vec::<usize>::new(), accounting.claim_owned(0, 0)?);
    let mut paths: Paths = (Vec::new(), accounting.claim_owned(0, 0)?);
    // Each new binding enters this stack once. Parent identities are indexed;
    // input ordering and directory depth cannot cause repeated whole scans.
    for start in 0..nodes.value.len() {
        let mut current = start;
        loop {
            accounting.charge_items(1)?;
            let node = nodes.value.get(current).ok_or(FsError::InvalidDiff)?;
            match node.state {
                CreationState::Complete(_) => break,
                CreationState::Visiting => return Err(FsError::InvalidDiff.into()),
                CreationState::Pending => {}
            }
            let parent = changes
                .bindings
                .get(node.binding)
                .ok_or(FsError::InvalidDiff)?
                .directory_id;
            creation_state(
                &mut nodes.value,
                current,
                CreationState::Visiting,
                accounting,
            )?;
            reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
            accounting.charge(copy_work(std::mem::size_of::<usize>() as u64))?;
            pending.0.push(current);
            let Some(parent) = creation_index(&nodes.value, parent, accounting)? else {
                break;
            };
            current = parent;
        }
        while let Some(index) = pending.0.pop() {
            accounting.charge_items(1)?;
            let node = nodes.value.get(index).ok_or(FsError::InvalidDiff)?;
            let binding = changes
                .bindings
                .get(node.binding)
                .ok_or(FsError::InvalidDiff)?;
            let parent = if let Some(parent) =
                creation_index(&nodes.value, binding.directory_id, accounting)?
            {
                let CreationState::Complete(output) =
                    nodes.value.get(parent).ok_or(FsError::InvalidDiff)?.state
                else {
                    return Err(FsError::InvalidDiff.into());
                };
                &paths.0.get(output).ok_or(FsError::InvalidDiff)?.path
            } else {
                let parent = binary_search_with_accounting(parents, accounting, |entry| {
                    entry.0.cmp(&binding.directory_id)
                })?
                .ok()
                .and_then(|index| parents.get(index))
                .ok_or(FsError::InvalidDiff)?;
                let [(path, _)] = parent.1.as_slice() else {
                    return Err(FsError::InvalidDiff.into());
                };
                path
            };
            let path =
                parent.child_with_accounting(&binding.name, limits, accounting, cancellation)?;
            let record = changed_file(changes, node.file_id, accounting)?
                .and_then(|file| file.after)
                .ok_or(FsError::InvalidDiff)?;
            let output = paths.0.len();
            paths::append(
                &mut paths,
                &path.0,
                Some(record),
                false,
                accounting,
                cancellation,
            )?;
            let bytes = path.0.retained_bytes();
            reverse_drop(path, bytes, accounting)?;
            creation_state(
                &mut nodes.value,
                index,
                CreationState::Complete(output),
                accounting,
            )?;
        }
    }
    let bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, bytes, accounting)?;
    nodes.retire(accounting)?;
    paths::sort_and_coalesce(&mut paths, accounting)?;
    Ok(paths)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(value: u128) -> FileId {
        FileId::from_bytes(value.to_be_bytes())
    }

    fn directory(file_id: FileId) -> crate::kernel::FileRecord {
        let object = |kind| crate::ObjectId {
            storage: None,
            kind,
            digest: crate::foundation::Digest::from_bytes([1; 32]),
        };
        crate::kernel::FileRecord {
            file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: object(crate::ObjectKind::Metadata),
            payload: FilePayload::Directory {
                entries: object(crate::ObjectKind::TreePage),
            },
        }
    }

    #[test]
    fn creation_paths_match_literal_namespace_with_finite_work_and_cycle_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = crate::model::VolumeLimits::default();
        let cancel = CancellationToken::new();
        let root = identity(0);
        let mut changes = GenerationDiff {
            files: Vec::new(),
            bindings: Vec::new(),
            truncated: false,
        };
        let mut expected = Vec::new();
        let mut nested = String::new();
        // Reversed file identities put descendants before their parents in
        // canonical binding order. The literal model uses only path strings.
        for number in 1..=2_112_u128 {
            let id = identity(2_113 - number);
            let parent = if (2..=64).contains(&number) {
                identity(2_114 - number)
            } else {
                root
            };
            let name = format!("entry-{number:04}");
            let path = if number <= 64 {
                nested.push('/');
                nested.push_str(&name);
                nested.clone()
            } else {
                format!("/{name}")
            };
            expected.push(path);
            let name = LogicalName::new(crate::kernel::NameEncoding::Utf8, name.into_bytes())?;
            changes.files.push(crate::kernel::FileRecordChange {
                file_id: id,
                before: None,
                after: Some(directory(id)),
            });
            changes
                .bindings
                .push(crate::kernel::DirectoryBindingChange {
                    directory_id: parent,
                    name: name.clone(),
                    before: None,
                    after: Some(crate::kernel::TreeEntry {
                        name,
                        file_id: id,
                        kind: FileKind::Directory,
                    }),
                });
        }
        changes.files.sort_by_key(|entry| entry.file_id);
        changes.bindings.sort_by(|left, right| {
            (left.directory_id, &left.name).cmp(&(right.directory_id, &right.name))
        });
        expected.sort();
        let parents = vec![(
            root,
            vec![(NamespacePath::new(Vec::new(), limits)?, directory(root))],
        )];
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        assert!(only_creates(&changes, &mut accounting)?);
        let paths = build_paths(&changes, &parents, limits, &mut accounting, &cancel)?;
        let actual = paths
            .0
            .iter()
            .map(|entry| namespace_path_text(&entry.path))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual, expected);
        let bytes = workspace_vec_bytes(&paths.0)
            + paths
                .0
                .iter()
                .map(|entry| entry.path.retained_bytes())
                .sum::<u64>();
        reverse_drop(paths, bytes, &mut accounting)?;
        // The first new directory now points back to its deepest child.
        let first = changes
            .bindings
            .iter_mut()
            .find(|entry| {
                entry
                    .after
                    .as_ref()
                    .is_some_and(|entry| entry.file_id == identity(2_112))
            })
            .ok_or("first binding missing")?;
        first.directory_id = identity(2_049);
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        assert!(matches!(
            build_paths(&changes, &parents, limits, &mut accounting, &cancel),
            Err(WorkspaceError::Filesystem(FsError::InvalidDiff))
        ));
        Ok(())
    }
}
