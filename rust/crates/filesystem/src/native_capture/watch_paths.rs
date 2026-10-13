use super::*;

#[derive(Clone, Copy)]
pub(super) enum CurrentRecord {
    Lookup,
    Known(Option<FileRecord>),
}

/// What a hinted path held before this batch: a renamed record, nothing if it was moved away, or the checkout's current binding.
pub(super) fn current_record(
    rename_records: &CaptureMap<FileRecord>,
    moved_away: &CaptureMap<()>,
    path: &NamespacePath,
    meter: &CaptureWorkScope,
) -> Result<CurrentRecord, CaptureError> {
    Ok(match rename_records.get_measured(path, meter)? {
        Some(record) => CurrentRecord::Known(Some(*record)),
        None if moved_away.contains_key_measured(path, meter)? => CurrentRecord::Known(None),
        None => CurrentRecord::Lookup,
    })
}

pub(super) fn watch_ancestor_candidates<'a>(
    paths: impl Iterator<Item = &'a NamespacePath>,
    ordinary: &CaptureMap<(CurrentRecord, CaptureIntent)>,
    rename_records: &CaptureMap<FileRecord>,
    policy: &CapturePolicy,
    maximum_paths: usize,
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
) -> Result<Vec<(NamespacePath, ())>, CaptureError> {
    let mut ancestors = CaptureMap::new();
    for path in paths {
        for depth in (1..path.depth()).rev() {
            let parent = clone_capture_prefix(path, depth, limits, meter)?;
            if !ordinary.contains_key_measured(&parent, meter)?
                && !rename_records.contains_key_measured(&parent, meter)?
                && !capture_excludes(policy, &parent, meter)?
                && !ancestors.contains_key_measured(&parent, meter)?
            {
                if combined_path_count_exceeds(
                    ancestors.len() + ordinary.len(),
                    rename_records.len() + 1,
                    maximum_paths,
                ) {
                    return Err(CaptureError::InvalidOptions);
                }
                ancestors.insert_measured(parent, (), meter)?;
            } else {
                capture_drop_path(parent, meter)?;
            }
        }
    }
    let mut ancestors = ancestors.entries;
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
    paths: CaptureMap<()>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let bytes = paths.storage_bytes();
    for (path, ()) in paths.entries {
        capture_drop_path(path, meter)?;
    }
    meter
        .native
        .clone()
        .release(bytes)
        .map_err(capture_demand_error)
}

pub(super) fn release_host_links(
    links: Vec<([u8; 16], NamespacePath)>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let bytes = links.capacity() as u64 * std::mem::size_of::<([u8; 16], NamespacePath)>() as u64;
    for (_, source) in links {
        capture_drop_path(source, meter)?;
    }
    meter
        .native
        .clone()
        .release(bytes)
        .map_err(capture_demand_error)
}

pub(super) fn capture_within_any(
    path: &NamespacePath,
    ancestors: &[NamespacePath],
    exclude_self: bool,
    meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    for ancestor in ancestors {
        capture_charge(
            meter,
            WorkCounters {
                items_examined: 2,
                source_path_components: path.depth().min(ancestor.depth()) as u64,
                ..WorkCounters::UNCHARGED
            },
        )?;
        if (!exclude_self || path != ancestor) && path.is_within(ancestor) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn hydrate_watch_ancestors<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<(CurrentRecord, CaptureIntent)>,
    ancestors: Vec<(NamespacePath, ())>,
    source_root: &HostRoot,
    epoch: u64,
    maximum_paths: usize,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut missing = Vec::new();
    let mut replaced = Vec::new();
    let ancestors_bytes =
        ancestors.capacity() as u64 * std::mem::size_of::<(NamespacePath, ())>() as u64;
    for (path, ()) in ancestors {
        let below_replaced = capture_within_any(&path, &replaced, false, meter)?;
        let current = if below_replaced {
            None
        } else {
            let lookup = checkout
                .lookup_no_follow(&path, &meter.native, cancellation)
                .await?;
            lookup.record
        };

        let host_path = namespace_to_host_path_measured(&path, meter)?;
        let observation = observe_host_path(source_root, &host_path, meter)?;
        capture_drop_host_path(host_path, meter)?;
        match observation {
            Some(observation) if observation.is_dir() => {
                if current.is_none_or(|record| record.kind != FileKind::Directory) {
                    if current.is_some() {
                        {
                            capture_reserve(&mut replaced, 1, meter)?;
                            replaced.push(clone_capture_path(&path, meter)?);
                            ()
                        };
                    }
                    {
                        capture_reserve(&mut missing, 1, meter)?;
                        missing.push(clone_capture_path(&path, meter)?);
                        ()
                    };
                    {
                        ordinary.insert_measured(
                            path,
                            (CurrentRecord::Known(current), CaptureIntent::Replace),
                            meter,
                        )
                    }?;
                } else {
                    capture_drop_path(path, meter)?;
                }
            }
            _ => {
                return Err(CaptureError::RescanRequired {
                    epoch: epoch,
                    reason: WatchInvalidationReason::NativeRescanRequired,
                });
            }
        }
    }
    meter
        .native
        .clone()
        .release(ancestors_bytes)
        .map_err(capture_demand_error)?;
    if ordinary.len() > maximum_paths {
        return Err(CaptureError::InvalidOptions);
    }

    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in &mut ordinary.entries {
        if matches!(current, CurrentRecord::Lookup)
            && capture_within_any(path, &replaced, true, meter)?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    release_capture_paths(replaced, meter)?;
    Ok(missing)
}

pub(super) fn remap_renamed_directory_path(
    path: &NamespacePath,
    renames: &[(NamespacePath, NamespacePath)],
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<(CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let maximum = usize::try_from(maximum_paths).map_err(|_| CaptureError::InvalidOptions)?;
    if ordinary.len() > maximum {
        return Err(CaptureError::InvalidOptions);
    }
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let mut roots = capture_vec(ordinary.len(), meter)?;
    for (path, (_, intent)) in &ordinary.entries {
        // Expand newly arrived directories; ordinary directory hints already name changed children.
        if !cfg!(target_os = "macos")
            && !matches!(intent, CaptureIntent::Replace | CaptureIntent::Arrived)
        {
            continue;
        }

        let host_path = namespace_to_host_path_measured(path, meter)?;
        let observation = observe_host_path(source_root, &host_path, meter)?;
        if observation.is_some_and(|observation| observation.is_dir()) {
            {
                roots.push((host_path, clone_capture_path(path, meter)?));
                ()
            };
        } else {
            capture_drop_host_path(host_path, meter)?;
        }
    }
    let mut paths = CaptureMap::new();
    for (path, _) in &ordinary.entries {
        {
            let path = clone_capture_path(path, meter)?;
            paths.insert_measured(path, (), meter).map(|_| ())
        }?;
    }
    let roots_bytes =
        roots.capacity() as u64 * std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    for (host_path, volume_path) in roots {
        let host_volume_path = clone_capture_path(&volume_path, meter)?;
        collect_host_walk_measured(
            source_root,
            profile,
            limits,
            host_path,
            host_volume_path,
            policy,
            meter,
            |path, _, meter| append_scanned_path(&mut paths, path, maximum, meter),
        )?;
        let lookup = checkout
            .lookup_no_follow(&volume_path, &meter.native, cancellation)
            .await?;
        if lookup
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(
                checkout,
                limits,
                volume_path,
                policy,
                maximum,
                &mut paths,
                cancellation,
                meter,
            )
            .await?;
        } else {
            capture_drop_path(volume_path, meter)?;
        }
    }
    meter
        .native
        .clone()
        .release(roots_bytes)
        .map_err(capture_demand_error)?;
    let paths_bytes = paths.storage_bytes();
    for (path, ()) in paths.entries {
        {
            ordinary
                .get_or_insert_measured(
                    path,
                    (CurrentRecord::Lookup, CaptureIntent::Complete),
                    meter,
                )
                .map(|_| ())
        }?;
    }
    meter
        .native
        .clone()
        .release(paths_bytes)
        .map_err(capture_demand_error)?;
    Ok(())
}
