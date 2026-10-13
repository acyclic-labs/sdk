use super::*;

pub(super) fn capture_watch_owned_bytes(
    changes: &[WatchChange],
    incoming: &crate::watch::WatchBatchOriginal,
    storage_bytes: u64,
    meter: &CaptureWorkScope,
) -> Result<u64, CaptureError> {
    let mut bytes = storage_bytes;
    for (index, change) in changes.iter().enumerate() {
        if incoming.change_has_native_payload_custody(index) {
            continue;
        }
        let (first, second) = match change {
            WatchChange::Created(path)
            | WatchChange::Modified(path)
            | WatchChange::Arrived(path)
            | WatchChange::Removed(path)
            | WatchChange::MetadataChanged(path) => (path, None),
            WatchChange::Renamed { from, to } => (from, Some(to)),
        };
        for path in std::iter::once(first).chain(second) {
            capture_charge(
                meter,
                WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            )?;
            for _ in path.components() {
                capture_charge(
                    meter,
                    WorkCounters {
                        source_path_components: 1,
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    },
                )?;
            }
            bytes = bytes
                .checked_add(path.retained_bytes())
                .ok_or(CaptureError::from(WorkError::Overflow))?;
        }
    }
    Ok(bytes)
}

/// Replays one watcher batch against the original shared invocation ledger.
#[allow(clippy::too_many_lines)]
pub(super) async fn capture_watch_batch_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<WatchCaptureReceipt, CaptureError> {
    // The facade opens fixed accounting leaves only; owned helpers and drops remain outside those short meter-meter loans.
    let cancellation = {
        cancellation.check()?;
        meter.native.clone().check().map_err(capture_demand_error)?;
        if let WatchBatch::Changes {
            changes,
            original: incoming,
            ..
        } = &batch
        {
            let storage_bytes = if incoming.has_native_changes_storage_custody() {
                0
            } else {
                (changes.capacity() as u64)
                    .checked_mul(std::mem::size_of::<WatchChange>() as u64)
                    .ok_or(CaptureError::from(WorkError::Overflow))?
            };
            let bytes = capture_watch_owned_bytes(changes, incoming, storage_bytes, meter)?;
            meter
                .native
                .with_original_leaf(|work| {
                    work.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED)
                })
                .map_err(capture_demand_error)?;
        }
        meter
            .native
            .with_original_leaf(|work| work.cancellation().clone())
    };
    let WatchBatch::Changes {
        original: incoming,
        epoch,
        first_sequence,
        next_sequence,
        changes,
    } = batch
    else {
        let WatchBatch::RescanRequired { epoch, reason } = batch else {
            unreachable!("watch batch variants are exhaustive")
        };
        return Err(CaptureError::RescanRequired {
            epoch: epoch,
            reason,
        });
    };
    let changes_storage_native = incoming.has_native_changes_storage_custody();
    // Drop moved hints before their meter allocation guards, including on early failure.
    let incoming = (changes, incoming);
    if options.maximum_paths == 0
        || options.maximum_extent_spans == 0
        || incoming.0.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
    {
        return Err(CaptureError::InvalidOptions);
    }
    let source_root = open_source_root(options, meter)?;
    let mut receipt = CaptureReceipt::default();
    let changes_bytes = incoming.0.capacity() as u64 * std::mem::size_of::<WatchChange>() as u64;
    let mut mutations = Vec::new();
    let mut ordinary = CaptureMap::new();
    let mut rename_records = CaptureMap::<FileRecord>::new();
    let mut renamed_directories = Vec::new();
    let mut rename_destinations = Vec::new();
    let mut moved_away = CaptureMap::new();

    for (index, change) in incoming.0.into_iter().enumerate() {
        let change = if incoming.1.change_has_native_payload_custody(index) {
            let copied = match &change {
                WatchChange::Created(path) => {
                    WatchChange::Created(clone_capture_path(path, meter)?)
                }
                WatchChange::Modified(path) => {
                    WatchChange::Modified(clone_capture_path(path, meter)?)
                }
                WatchChange::Arrived(path) => {
                    WatchChange::Arrived(clone_capture_path(path, meter)?)
                }
                WatchChange::Removed(path) => {
                    WatchChange::Removed(clone_capture_path(path, meter)?)
                }
                WatchChange::MetadataChanged(path) => {
                    WatchChange::MetadataChanged(clone_capture_path(path, meter)?)
                }
                WatchChange::Renamed { from, to } => WatchChange::Renamed {
                    from: clone_capture_path(from, meter)?,
                    to: clone_capture_path(to, meter)?,
                },
            };
            drop(change);
            copied
        } else {
            change
        };
        {
            capture_charge(
                meter,
                WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            )
        }?;
        let change = match change {
            WatchChange::Renamed { from, to } => {
                let excluded_from = capture_excludes(policy, &from, meter)?;
                let excluded_to = capture_excludes(policy, &to, meter)?;
                match (excluded_from, excluded_to) {
                    (false, false) => WatchChange::Renamed { from, to },
                    (true, false) => {
                        capture_drop_path(from, meter)?;
                        WatchChange::Created(to)
                    }
                    (false, true) => {
                        capture_drop_path(to, meter)?;
                        WatchChange::Removed(from)
                    }
                    (true, true) => {
                        capture_drop_path(from, meter)?;
                        capture_drop_path(to, meter)?;
                        continue;
                    }
                }
            }
            change => {
                let path = match &change {
                    WatchChange::Created(path)
                    | WatchChange::Modified(path)
                    | WatchChange::Arrived(path)
                    | WatchChange::Removed(path)
                    | WatchChange::MetadataChanged(path) => path,
                    WatchChange::Renamed { .. } => unreachable!("renames were classified above"),
                };
                if capture_excludes(policy, path, meter)? {
                    let bytes = path.retained_bytes();
                    drop(change);
                    meter
                        .native
                        .clone()
                        .release(bytes)
                        .map_err(capture_demand_error)?;
                    continue;
                }
                change
            }
        };
        match change {
            WatchChange::Created(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary.insert_measured(path, (current, CaptureIntent::Replace), meter)?;
            }
            WatchChange::Modified(path) | WatchChange::Removed(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary
                    .get_or_insert_measured(path, (current, CaptureIntent::Complete), meter)
                    .map(|_| ())?;
            }
            WatchChange::Arrived(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                {
                    let entry = ordinary.get_or_insert_measured(
                        path,
                        (current, CaptureIntent::Arrived),
                        meter,
                    )?;
                    if entry.1 != CaptureIntent::Replace {
                        entry.1 = CaptureIntent::Arrived;
                    }
                    ()
                };
            }
            WatchChange::MetadataChanged(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary
                    .get_or_insert_measured(path, (current, CaptureIntent::MetadataOnly), meter)
                    .map(|_| ())?;
            }
            WatchChange::Renamed { from, to } => {
                cancellation.check()?;
                let prior_source = ordinary.remove_measured(&from, meter)?;
                let prior_source = if let Some((key, value)) = prior_source {
                    capture_drop_path(key, meter)?;
                    Some(value)
                } else {
                    None
                };
                if let Some((key, _)) = ordinary.remove_measured(&to, meter)? {
                    capture_drop_path(key, meter)?;
                }
                let record =
                    if let Some((key, record)) = rename_records.remove_measured(&from, meter)? {
                        capture_drop_path(key, meter)?;
                        Some(record)
                    } else if matches!(prior_source, Some((CurrentRecord::Known(None), _)))
                        || moved_away.contains_key_measured(&from, meter)?
                    {
                        None
                    } else {
                        let source = checkout
                            .lookup_no_follow(&from, &meter.native, &cancellation)
                            .await?;
                        source.record
                    };
                if let Some(record) = record {
                    if record.kind == FileKind::Directory {
                        capture_reserve(&mut renamed_directories, 1, meter)?;
                        renamed_directories.push((
                            clone_capture_path(&from, meter)?,
                            clone_capture_path(&to, meter)?,
                        ));
                    }
                    capture_reserve(&mut rename_destinations, 1, meter)?;
                    rename_destinations.push(clone_capture_path(&to, meter)?);
                    capture_reserve(&mut mutations, 1, meter)?;
                    mutations.push(AuthoredMutation::Rename {
                        source: clone_capture_path(&from, meter)?,
                        destination: clone_capture_path(&to, meter)?,
                        replace: true,
                    });
                    let destination = clone_capture_path(&to, meter)?;
                    rename_records.insert_measured(destination, record, meter)?;
                } else {
                    let destination = clone_capture_path(&to, meter)?;
                    {
                        ordinary.insert_measured(
                            destination,
                            (CurrentRecord::Lookup, CaptureIntent::Replace),
                            meter,
                        )
                    }?;
                }
                moved_away.insert_measured(from, (), meter)?;
                if let Some((key, ())) = moved_away.remove_measured(&to, meter)? {
                    capture_drop_path(key, meter)?;
                }
                capture_drop_path(to, meter)?;
            }
        }
    }
    if !changes_storage_native {
        meter
            .native
            .clone()
            .release(changes_bytes)
            .map_err(capture_demand_error)?;
    }
    drop(incoming.1);
    release_capture_set(moved_away, meter)?;

    expand_directory_hints(
        checkout,
        &mut ordinary,
        &source_root,
        options.maximum_paths,
        policy,
        &cancellation,
        meter,
    )
    .await?;

    // A watcher may report a nested file before any ancestor has been observed in this lazy checkout. Capture only the missing ancestors, rather than expanding their entire host subtrees as event roots.
    let maximum_paths = usize::try_from(options.maximum_paths).unwrap_or(usize::MAX);
    let ancestors = watch_ancestor_candidates(
        ordinary
            .entries
            .iter()
            .map(|(path, _)| path)
            .chain(rename_records.entries.iter().map(|(path, _)| path))
            .chain(rename_destinations.iter()),
        &ordinary,
        &rename_records,
        policy,
        maximum_paths,
        checkout.volume_config().limits,
        meter,
    )?;
    release_capture_paths(rename_destinations, meter)?;
    let mut missing_ancestors = hydrate_watch_ancestors(
        checkout,
        &mut ordinary,
        ancestors,
        &source_root,
        epoch,
        maximum_paths.saturating_sub(rename_records.len()),
        &cancellation,
        meter,
    )
    .await?;

    let mut lookup_paths = capture_vec(ordinary.len(), meter)?;
    for (path, (current, _)) in &ordinary.entries {
        if matches!(current, CurrentRecord::Lookup) {
            lookup_paths.push(clone_capture_path(path, meter)?);
        }
    }
    if !lookup_paths.is_empty() {
        let lookup = checkout
            .lookup_batch_no_follow(&lookup_paths, &meter.native, &cancellation)
            .await?;

        let lookup_paths_bytes =
            lookup_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for (path, entry) in lookup_paths.into_iter().zip(lookup.0.entries) {
            let mut record = entry.record;
            if record.is_none()
                && let Some(prior_path) = {
                    remap_renamed_directory_path(
                        &path,
                        &renamed_directories,
                        checkout.volume_config().limits,
                        meter,
                    )
                }?
            {
                let prior = checkout
                    .lookup_no_follow(&prior_path, &meter.native, &cancellation)
                    .await?;
                record = prior.record;
                capture_drop_path(prior_path, meter)?;
            }
            let Ok(index) = ordinary.position_measured(&path, meter)? else {
                return Err(CaptureError::InvalidOptions);
            };
            let (current, _) = &mut ordinary.entries[index].1;
            *current = CurrentRecord::Known(record);
            capture_drop_path(path, meter)?;
        }
        drop(lookup.1);
        meter
            .native
            .clone()
            .release(lookup_paths_bytes)
            .map_err(capture_demand_error)?;
    } else {
        release_capture_paths(lookup_paths, meter)?;
    }
    {
        let storage_bytes = renamed_directories.capacity() as u64
            * std::mem::size_of::<(NamespacePath, NamespacePath)>() as u64;
        for (from, to) in renamed_directories {
            capture_drop_path(from, meter)?;
            capture_drop_path(to, meter)?;
        }
        meter
            .native
            .clone()
            .release(storage_bytes)
            .map_err(capture_demand_error)
    }?;

    let mut host_links = Vec::new();
    // A rename can target a directory that has never been observed in this lazy checkout. Materialize only those host ancestors before the rename, preserving the event order for all remaining mutations.
    {
        capture_sort_by(
            &mut missing_ancestors,
            |left, right| {
                left.depth()
                    .cmp(&right.depth())
                    .then_with(|| left.cmp(right))
            },
            meter,
        )
    }?;
    let rename_mutations = std::mem::take(&mut mutations);
    let missing_bytes =
        missing_ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in missing_ancestors {
        let Some((key, (CurrentRecord::Known(current), intent))) =
            ordinary.remove_measured(&path, meter)?
        else {
            return Err(CaptureError::InvalidOptions);
        };
        capture_drop_path(key, meter)?;
        prepare_final_path(
            checkout,
            path,
            current,
            intent,
            &source_root,
            None,
            &mut host_links,
            Some(epoch),
            None,
            &mut mutations,
            &mut receipt,
            options.maximum_extent_spans,
            &cancellation,
            meter,
        )
        .await?;
    }
    meter
        .native
        .clone()
        .release(missing_bytes)
        .map_err(capture_demand_error)?;
    let rename_mutations_bytes =
        rename_mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
    capture_reserve(&mut mutations, rename_mutations.len(), meter)?;
    {
        capture_charge(
            meter,
            WorkCounters {
                bytes_copied: rename_mutations.len() as u64
                    * std::mem::size_of::<AuthoredMutation>() as u64,
                ..WorkCounters::UNCHARGED
            },
        )
    }?;
    mutations.extend(rename_mutations);
    meter
        .native
        .clone()
        .release(rename_mutations_bytes)
        .map_err(capture_demand_error)?;
    let namespace_boundary = mutations.len();
    let rename_records_bytes = rename_records.storage_bytes();
    for (path, record) in rename_records.entries {
        if ordinary.contains_key_measured(&path, meter)? {
            capture_drop_path(path, meter)?;
            continue;
        }
        prepare_final_path(
            checkout,
            path,
            Some(record),
            CaptureIntent::Complete,
            &source_root,
            None,
            &mut host_links,
            Some(epoch),
            None,
            &mut mutations,
            &mut receipt,
            options.maximum_extent_spans,
            &cancellation,
            meter,
        )
        .await?;
    }
    meter
        .native
        .clone()
        .release(rename_records_bytes)
        .map_err(capture_demand_error)?;
    let mut observed_ordinary = capture_vec(ordinary.len(), meter)?;
    let ordinary_bytes = ordinary.storage_bytes();
    for (path, (current, intent)) in ordinary.entries {
        let CurrentRecord::Known(current) = current else {
            return Err(CaptureError::InvalidOptions);
        };
        let host_path = namespace_to_host_path_measured(&path, meter)?;
        let observation = observe_host_path(&source_root, &host_path, meter)?;
        capture_drop_host_path(host_path, meter)?;
        observed_ordinary.push((path, current, intent, observation));
    }
    meter
        .native
        .clone()
        .release(ordinary_bytes)
        .map_err(capture_demand_error)?;
    let mut ordinary = observed_ordinary;
    {
        capture_sort_by(
            &mut ordinary,
            |left, right| match (left.3.is_some(), right.3.is_some()) {
                (true, true) => left
                    .0
                    .depth()
                    .cmp(&right.0.depth())
                    .then_with(|| left.0.cmp(&right.0)),
                (false, false) => right
                    .0
                    .depth()
                    .cmp(&left.0.depth())
                    .then_with(|| left.0.cmp(&right.0)),
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
            },
            meter,
        )
    }?;
    let observed_bytes = ordinary.capacity() as u64
        * std::mem::size_of::<(
            NamespacePath,
            Option<FileRecord>,
            CaptureIntent,
            Option<HostStat>,
        )>() as u64;
    for (path, current, intent, observation) in ordinary {
        prepare_final_path(
            checkout,
            path,
            current,
            intent,
            &source_root,
            observation,
            &mut host_links,
            Some(epoch),
            None,
            &mut mutations,
            &mut receipt,
            options.maximum_extent_spans,
            &cancellation,
            meter,
        )
        .await?;
    }
    meter
        .native
        .clone()
        .release(observed_bytes)
        .map_err(capture_demand_error)?;
    release_host_links(host_links, meter)?;
    if namespace_boundary == 0 || namespace_boundary == mutations.len() {
        apply_capture_transaction(checkout, mutations, meter).await?;
    } else {
        // Compile binding changes before final-state mutations on one atomic candidate.
        let mut candidate = checkout.private_candidate(&meter.native, &cancellation)?;
        let final_state = {
            let count = mutations.len() - namespace_boundary;
            let bytes = count
                .checked_mul(std::mem::size_of::<AuthoredMutation>())
                .ok_or(CaptureError::from(WorkError::Overflow))? as u64;
            let copied = WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            };
            crate::native_host::NativeHostWork::admit(&mut meter.native.clone(), copied)
                .map_err(capture_demand_error)?;
            let mut final_state = capture_vec(count, meter)?;
            final_state.extend(mutations.drain(namespace_boundary..));
            meter
                .native
                .clone()
                .record(copied)
                .map_err(capture_demand_error)?;
            final_state
        };
        apply_capture_transaction(&mut candidate, mutations, meter).await?;
        apply_capture_transaction(&mut candidate, final_state, meter).await?;
        {
            cancellation.check()?;
            meter.native.clone().check().map_err(capture_demand_error)
        }?;
        cancellation.commit_if_live(|| {
            *checkout = candidate;
        })?;
    }
    Ok(WatchCaptureReceipt {
        epoch,
        first_sequence,
        next_sequence,
        capture: receipt,
    })
}
