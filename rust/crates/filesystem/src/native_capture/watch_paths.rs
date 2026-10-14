use super::*;

#[derive(Clone, Copy)]
pub(super) enum CurrentRecord {
    Lookup,
    Known(Option<FileRecord>),
}

/// What a hinted path held before this batch: a renamed record, nothing if it was moved away, or the checkout's current binding.
pub(super) fn current_record(
    rename_records: &[(NamespacePath, Option<FileRecord>)],
    path: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<CurrentRecord, CaptureError> {
    Ok(match capture_position(rename_records, path, meter)? {
        Ok(index) => CurrentRecord::Known(rename_records.get(index).ok_or(WorkError::Overflow)?.1),
        Err(_) => CurrentRecord::Lookup,
    })
}

pub(super) fn watch_ancestor_candidates<'a>(
    paths: impl Iterator<Item = &'a NamespacePath>,
    ordinary: &[(NamespacePath, (CurrentRecord, CaptureIntent))],
    rename_records: &[(NamespacePath, Option<FileRecord>)],
    policy: &CapturePolicy,
    maximum_paths: usize,
    limits: crate::model::VolumeLimits,
    meter: &NativeMaterializationWork,
) -> Result<Vec<(NamespacePath, ())>, CaptureError> {
    let mut ancestors = Vec::new();
    for path in paths {
        for depth in (1..path.depth()).rev() {
            let parent = clone_capture_prefix(path, depth, limits, meter)?;
            if capture_position(ordinary, &parent, meter)?.is_err()
                && match capture_position(rename_records, &parent, meter)? {
                    Ok(index) => rename_records
                        .get(index)
                        .ok_or(WorkError::Overflow)?
                        .1
                        .is_none(),
                    Err(_) => true,
                }
                && !capture_excludes(policy, &parent, meter)?
                && capture_position(&ancestors, &parent, meter)?.is_err()
            {
                if combined_path_count_exceeds(ancestors.len() + ordinary.len(), 1, maximum_paths) {
                    return Err(CaptureError::InvalidOptions);
                }
                capture_replace(&mut ancestors, parent, (), meter)?;
            } else {
                capture_drop_path(parent, meter)?;
            }
        }
    }
    capture_sort_by(
        &mut ancestors,
        |left, right| {
            left.0
                .depth()
                .cmp(&right.0.depth())
                .then_with(|| left.0.cmp(&right.0))
        },
        meter,
    )?;
    Ok(ancestors)
}

pub(super) fn release_capture_set(
    paths: Vec<(NamespacePath, ())>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let bytes = vector_bytes(&paths);
    for (path, ()) in paths {
        capture_drop_path(path, meter)?;
    }
    meter.clone().release(bytes).map_err(capture_demand_error)
}

pub(super) fn release_host_links(
    links: Vec<([u8; 16], NamespacePath)>,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let bytes = links.capacity() as u64 * std::mem::size_of::<([u8; 16], NamespacePath)>() as u64;
    for (_, source) in links {
        capture_drop_path(source, meter)?;
    }
    meter.clone().release(bytes).map_err(capture_demand_error)
}

fn below_replaced_parent(
    path: &NamespacePath,
    parents: &[(NamespacePath, Option<FileRecord>)],
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    for (parent, record) in parents {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        if record.is_some() {
            capture_charge(
                meter,
                WorkCounters {
                    items_examined: 1,
                    source_path_components: path.depth().min(parent.depth()) as u64,
                    ..WorkCounters::UNCHARGED
                },
            )?;
            if path != parent && path.is_within(parent) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) async fn hydrate_watch_ancestors<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut [(NamespacePath, (CurrentRecord, CaptureIntent))],
    ancestors: Vec<(NamespacePath, ())>,
    source_root: &HostRoot,
    epoch: u64,
    maximum_paths: usize,
    meter: &NativeMaterializationWork,
) -> Result<Vec<(NamespacePath, Option<FileRecord>)>, CaptureError> {
    let mut missing = Vec::new();
    let ancestors_bytes = vector_bytes(&ancestors);
    for (path, ()) in ancestors {
        let current = if below_replaced_parent(&path, &missing, meter)? {
            None
        } else {
            checkout
                .lookup_no_follow(&path, meter, meter.cancellation())
                .await?
                .record
        };
        let host_path = namespace_to_host_path_measured(&path, meter)?;
        let observation = observe_host_path(source_root, &host_path, meter)?;
        capture_drop_host_path(host_path, meter)?;
        if !observation.is_some_and(|observation| observation.is_dir()) {
            return Err(CaptureError::RescanRequired {
                epoch,
                reason: WatchInvalidationReason::NativeRescanRequired,
            });
        }
        if current.is_some_and(|record| record.kind == FileKind::Directory) {
            capture_drop_path(path, meter)?;
        } else {
            if combined_path_count_exceeds(ordinary.len(), missing.len() + 1, maximum_paths) {
                return Err(CaptureError::InvalidOptions);
            }
            capture_reserve(&mut missing, 1, meter)?;
            missing.push((path, current));
        }
    }
    meter
        .clone()
        .release(ancestors_bytes)
        .map_err(capture_demand_error)?;
    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in ordinary.iter_mut() {
        if matches!(current, CurrentRecord::Lookup) && below_replaced_parent(path, &missing, meter)?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    Ok(missing)
}

pub(super) fn remap_renamed_directory_path(
    path: &NamespacePath,
    renames: &[(NamespacePath, NamespacePath)],
    limits: crate::model::VolumeLimits,
    meter: &NativeMaterializationWork,
) -> Result<Option<NamespacePath>, CaptureError> {
    let mut prior = clone_capture_path(path, meter)?;
    for (from, to) in renames.iter().rev() {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 1,
                source_path_components: prior.depth() as u64,
                ..WorkCounters::UNCHARGED
            },
        )?;
        if prior == *to || !prior.is_within(to) {
            continue;
        }
        let suffix = prior
            .components()
            .get(to.depth()..)
            .ok_or(CaptureError::InvalidOptions)?;
        let mut components = capture_vec(from.depth() + suffix.len(), meter)?;
        for name in from.components().iter().chain(suffix) {
            components.push(clone_capture_name(name, meter)?);
        }
        let replacement = NamespacePath::new(components, limits).map_err(CaptureError::from)?;
        let previous = std::mem::replace(&mut prior, replacement);
        capture_drop_path(previous, meter)?;
    }
    if prior == *path {
        capture_drop_path(prior, meter)?;
        Ok(None)
    } else {
        Ok(Some(prior))
    }
}

pub(super) async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut Vec<(NamespacePath, (CurrentRecord, CaptureIntent))>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let maximum = usize::try_from(maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
    if ordinary.len() > maximum {
        return Err(CaptureError::InvalidOptions);
    }
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let mut paths = Vec::new();
    for (path, _) in ordinary.iter() {
        capture_replace(&mut paths, clone_capture_path(path, meter)?, (), meter)?;
    }
    for (path, (_, intent)) in ordinary.iter() {
        // Expand arriving directories; other hints already name their changed children.
        if !cfg!(target_os = "macos")
            && !matches!(intent, CaptureIntent::Replace | CaptureIntent::Arrived)
        {
            continue;
        }
        let host_path = namespace_to_host_path_measured(path, meter)?;
        let observation = observe_host_path(source_root, &host_path, meter)?;
        if !observation.is_some_and(|observation| observation.is_dir()) {
            capture_drop_host_path(host_path, meter)?;
            continue;
        }
        capture_drop_host_path(host_path, meter)?;
        collect_host_walk_measured(
            source_root,
            profile,
            limits,
            clone_capture_path(path, meter)?,
            policy,
            meter,
            |path, _, meter| append_scanned_path(&mut paths, path, maximum, meter),
        )?;
        let lookup = checkout
            .lookup_no_follow(path, meter, meter.cancellation())
            .await?;
        if lookup
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(
                checkout,
                limits,
                clone_capture_path(path, meter)?,
                policy,
                maximum,
                &mut paths,
                meter,
            )
            .await?;
        }
    }
    let paths_bytes = vector_bytes(&paths);
    for (path, ()) in paths {
        capture_entry(
            ordinary,
            path,
            (CurrentRecord::Lookup, CaptureIntent::Complete),
            meter,
        )?;
    }
    meter
        .clone()
        .release(paths_bytes)
        .map_err(capture_demand_error)?;
    Ok(())
}
