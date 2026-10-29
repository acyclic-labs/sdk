use super::*;

pub(super) fn order_host_checkout_union(
    mut observed: Vec<(NamespacePath, Option<HostStat>)>,
    mut checkout_paths: Vec<(NamespacePath, ())>,
    maximum: Option<usize>,
    meter: &NativeMaterializationWork,
) -> Result<(Vec<Option<HostStat>>, Vec<NamespacePath>), CaptureError> {
    capture_order_entries(&mut checkout_paths, maximum, meter)?;
    let checkout_bytes = vector_bytes(&checkout_paths);
    let observed_bytes = vector_bytes(&observed);
    // Count only checkout paths missing from the host, using one monotone scan.
    let mut host = observed.iter().peekable();
    let mut count = observed.len();
    for (path, ()) in &checkout_paths {
        while let Some((candidate, _)) = host.peek() {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(2),
            )?;
            if candidate < path {
                host.next();
            } else {
                break;
            }
        }
        if host.peek().is_none_or(|(candidate, _)| candidate != path) {
            count = count.checked_add(1).ok_or(WorkError::Overflow)?;
        }
    }
    if maximum.is_some_and(|maximum| count > maximum) {
        return Err(CaptureError::InvalidOptions);
    }
    let mut observations = capture_vec(count, meter)?;
    let mut paths = capture_vec(count, meter)?;
    let mut host = observed.drain(..).peekable();
    let mut checkout = checkout_paths.into_iter().peekable();
    loop {
        meter.clone().check().map_err(capture_demand_error)?;
        let (path, observation) = match (host.peek(), checkout.peek()) {
            (Some((left, _)), Some((right, ()))) => {
                capture_charge(
                    meter,
                    crate::materializer::NativeResourceCredit::Items.delta(2),
                )?;
                match left.cmp(right) {
                    std::cmp::Ordering::Less => host.next().ok_or(WorkError::Overflow)?,
                    std::cmp::Ordering::Greater => {
                        let (path, ()) = checkout.next().ok_or(WorkError::Overflow)?;
                        (path, None)
                    }
                    std::cmp::Ordering::Equal => {
                        let (duplicate, ()) = checkout.next().ok_or(WorkError::Overflow)?;
                        capture_drop_path(duplicate, meter)?;
                        host.next().ok_or(WorkError::Overflow)?
                    }
                }
            }
            (Some(_), None) => host.next().ok_or(WorkError::Overflow)?,
            (None, Some(_)) => {
                let (path, ()) = checkout.next().ok_or(WorkError::Overflow)?;
                (path, None)
            }
            (None, None) => break,
        };
        observations.push(observation);
        paths.push(path);
    }
    drop(host);
    drop(checkout);
    drop(observed);
    meter
        .clone()
        .release(checkout_bytes)
        .map_err(capture_demand_error)?;
    meter
        .clone()
        .release(observed_bytes)
        .map_err(capture_demand_error)?;
    Ok((observations, paths))
}

pub(super) fn combined_path_count_exceeds(
    observed: usize,
    checkout_only: usize,
    maximum: Option<usize>,
) -> bool {
    observed
        .checked_add(checkout_only)
        .is_none_or(|combined| maximum.is_some_and(|maximum| combined > maximum))
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
        #[cfg(all(test, feature = "memory"))]
        let move_leaf = move_leaf && !super::leaf_path_tests::clone_leaves();
        if move_leaf {
            meter.clone().check().map_err(capture_demand_error)?;
            return capture_push_entry(paths, child, (), meter);
        }
        let scanned = clone_capture_path(&child, meter)?;
        capture_push_entry(paths, scanned, (), meter)?;
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

pub(super) fn validate_path_count(
    paths: &[NamespacePath],
    options: &CaptureOptions,
) -> Result<(), CaptureError> {
    if options.maximum_paths == Some(0)
        || options.maximum_extent_spans == Some(0)
        || options
            .maximum_paths
            .is_some_and(|maximum| paths.len() as u64 > u64::from(maximum))
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
