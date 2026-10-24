//! A directory binding moves its subtree, never the unrelated namespace.
use super::*;

pub(super) async fn expand<O: AsyncObjectStore>(
    endpoints: &[super::super::namespace_view::NamespaceView<'_, O>; 2],
    changes: &GenerationDiff,
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
    let seeds =
        resolve_changed_paths(endpoints, changes, u32::MAX, accounting, cancellation).await?;
    let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
    for (index, view) in endpoints.iter().enumerate() {
        append_endpoint(
            view,
            &seeds.0,
            index == 0,
            &mut paths,
            accounting,
            cancellation,
        )
        .await?;
    }
    let bytes = workspace_vec_bytes(&seeds.0)
        + seeds
            .0
            .iter()
            .map(|entry| entry.path.retained_bytes())
            .sum::<u64>();
    reverse_drop(seeds, bytes, accounting)?;
    paths::sort_and_coalesce(&mut paths, accounting)?;
    let paths = retain_changed_paths(paths, accounting)?;
    if paths.0.len() > maximum_entries as usize {
        return Err(WorkspaceError::ChangedPathLimit);
    }
    Ok(paths)
}

async fn append_endpoint<O: AsyncObjectStore>(
    view: &super::super::namespace_view::NamespaceView<'_, O>,
    seeds: &[ChangedPath],
    before: bool,
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    let mut expanded: Option<&NamespacePath> = None;
    for seed in seeds {
        accounting.charge_items(1)?;
        let record = if before { seed.before } else { seed.after };
        let Some(record) = record else { continue };
        paths::append(
            paths,
            &seed.path,
            Some(record),
            before,
            accounting,
            cancellation,
        )?;
        let identity = |record: &crate::kernel::FileRecord| (record.file_id, record.kind);
        if record.kind != FileKind::Directory
            || seed.before.as_ref().map(identity) == seed.after.as_ref().map(identity)
            || expanded.is_some_and(|root| seed.path.components().starts_with(root.components()))
        {
            continue;
        }
        // Seeds are path-sorted. A selected ancestor owns every nested seed's
        // expansion, so overlapping bindings cannot repeatedly walk a subtree.
        let descendants = view
            .descendants(&seed.path, u32::MAX, accounting, cancellation)
            .await?;
        if !descendants.0.complete {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        for (_, records) in &descendants.0.records {
            for (path, record) in records {
                accounting.charge_items(1)?;
                paths::append(paths, path, Some(*record), before, accounting, cancellation)?;
            }
        }
        let bytes = namespace_records_bytes(&descendants.0.records);
        reverse_drop(descendants, bytes, accounting)?;
        expanded = Some(&seed.path);
    }
    Ok(())
}
