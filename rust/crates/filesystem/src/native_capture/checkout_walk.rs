use super::*;

pub(super) fn order_host_checkout_union(
    mut observed: Vec<(NamespacePath, Option<HostStat>)>,
    checkout_paths: Vec<(NamespacePath, ())>,
    maximum: usize,
    meter: &NativeMaterializationWork,
) -> Result<(Vec<Option<HostStat>>, Vec<NamespacePath>), CaptureError> {
    let checkout_bytes = vector_bytes(&checkout_paths);
    for (path, ()) in checkout_paths {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        match capture_position(&observed, &path, meter)? {
            Ok(_) => capture_drop_path(path, meter)?,
            Err(index) => {
                if observed.len() >= maximum {
                    return Err(CaptureError::InvalidOptions);
                }
                capture_insert_at(&mut observed, index, path, None, meter)?;
            }
        }
    }
    meter
        .clone()
        .release(checkout_bytes)
        .map_err(capture_demand_error)?;
    let observed_bytes = vector_bytes(&observed);
    let mut observations = capture_vec(observed.len(), meter)?;
    let mut paths = capture_vec(observed.len(), meter)?;
    for (path, observation) in observed {
        meter.clone().check().map_err(capture_demand_error)?;
        observations.push(observation);
        paths.push(path);
    }
    meter
        .clone()
        .release(observed_bytes)
        .map_err(capture_demand_error)?;
    Ok((observations, paths))
}

pub(super) fn combined_path_count_exceeds(
    observed: usize,
    checkout_only: usize,
    maximum: usize,
) -> bool {
    observed
        .checked_add(checkout_only)
        .is_none_or(|combined| combined > maximum)
}

pub(super) async fn walk_checkout_subtree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: NamespacePath,
    page_size: u32,
    meter: &NativeMaterializationWork,
    mut visit: impl FnMut(
        &NamespacePath,
        &crate::DirectoryRecordEntry,
        &mut Vec<NamespacePath>,
    ) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let mut pending = capture_vec(1, meter)?;
    pending.push(root);
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let cancellation = meter.cancellation().clone();
            let page = checkout
                .list_directory_records(
                    &directory,
                    after
                        .as_ref()
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    page_size,
                    meter,
                    &cancellation,
                )
                .await?;
            capture_charge(
                meter,
                WorkCounters {
                    items_examined: page.0.entries.len() as u64,
                    ..WorkCounters::UNCHARGED
                },
            )?;
            if page.0.has_more && page.0.entries.is_empty() {
                return Err(CaptureError::InvalidOptions);
            }
            for entry in &page.0.entries {
                visit(&directory, entry, &mut pending)?;
            }
            let next = if page.0.has_more {
                let last = page.0.entries.last().ok_or(CaptureError::InvalidOptions)?;
                Some(clone_capture_name(&last.name, meter)?)
            } else {
                None
            };
            if let Some(previous) = after.take() {
                let bytes = previous.retained_bytes() as u64;
                drop(previous);
                meter.clone().release(bytes).map_err(capture_demand_error)?;
            }
            drop(page);
            after = next;
            if after.is_none() {
                break;
            }
        }
        capture_drop_path(directory, meter)?;
    }
    release_capture_paths(pending, meter)
}

pub(super) async fn collect_checkout_subtree_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    root: NamespacePath,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut Vec<(NamespacePath, ())>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    meter.cancellation().check()?;
    walk_checkout_subtree(checkout, root, 1024, meter, |directory, entry, pending| {
        capture_charge(
            meter,
            WorkCounters {
                source_entries_visited: 1,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let name = clone_capture_name(&entry.name, meter)?;
        let child = append_path_measured(directory, name, limits, meter)?;
        if capture_excludes(policy, &child, meter)? {
            return capture_drop_path(child, meter);
        }
        let move_leaf = entry.record.kind != FileKind::Directory;
        #[cfg(test)]
        #[cfg(feature = "memory")]
        let move_leaf = move_leaf && !super::leaf_path_tests::clone_leaves();
        if move_leaf {
            meter.clone().check().map_err(capture_demand_error)?;
            return append_scanned_path(paths, child, maximum, meter);
        }
        let scanned = clone_capture_path(&child, meter)?;
        append_scanned_path(paths, scanned, maximum, meter)?;
        if entry.record.kind == FileKind::Directory {
            capture_reserve(pending, 1, meter)?;
            pending.push(child);
        } else {
            capture_drop_path(child, meter)?;
        }
        Ok(())
    })
    .await
}

pub(super) fn append_scanned_path(
    paths: &mut Vec<(NamespacePath, ())>,
    path: NamespacePath,
    maximum: usize,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let Err(index) = capture_position(paths, &path, meter)? else {
        return capture_drop_path(path, meter);
    };
    if paths.len() >= maximum {
        return Err(CaptureError::InvalidOptions);
    }
    capture_charge(
        meter,
        WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    capture_insert_at(paths, index, path, (), meter)
}

pub(super) fn validate_path_count(
    paths: &[NamespacePath],
    options: &CaptureOptions,
) -> Result<(), CaptureError> {
    if options.maximum_paths == 0
        || options.maximum_extent_spans == 0
        || paths.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
    {
        return Err(CaptureError::InvalidOptions);
    }
    Ok(())
}

pub(super) fn open_source_root(
    options: &CaptureOptions,
    meter: &NativeMaterializationWork,
) -> Result<HostRoot, CaptureError> {
    let root =
        HostRoot::open(&options.source_root, &mut meter.clone()).map_err(capture_demand_error)?;
    if root.identity() != options.expected_root_identity {
        return Err(CaptureError::RootChanged);
    }
    Ok(root)
}
