use super::*;

/// Replays one watcher batch with the caller's full original native resource.
#[allow(
    clippy::too_many_lines,
    reason = "Admission, rename validation, replay and commit share one funded candidate frame."
)]
pub async fn capture_watch_batch<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    W: crate::facade::FsWork,
>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: W,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let accounting = budget.accounting(cancellation);
    let native = match accounting.native_work() {
        Some(original) => original.clone(),
        None => capture_work(
            accounting
                .remaining()
                .map_err(|error| OperationFailure::new(error.into(), accounting.spent()))?,
            cancellation,
        )?,
    };
    let mut candidate = checkout
        .private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&native, cancellation),
            cancellation,
        )
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    let scope = CaptureWorkScope::new(&native)?;
    let result = async {
        let checkout = &mut candidate;
        let meter = &scope.native;
        cancellation.check()?;
        meter.clone().check().map_err(capture_demand_error)?;
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
            return Err(CaptureError::RescanRequired { epoch, reason });
        };
        let changes_storage_native = incoming.has_native_changes_storage_custody();
        // Drop moved hints before their meter allocation guards, including on early failure.
        let incoming = (changes, incoming);
        let changes_bytes = vector_bytes(&incoming.0);
        let mut bytes = if changes_storage_native {
            0
        } else {
            changes_bytes
        };
        for (index, change) in incoming.0.iter().enumerate() {
            if incoming.1.change_has_native_payload_custody(index) {
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
                        items_examined: 1 + path.depth() as u64,
                        source_path_components: path.depth() as u64,
                        ..WorkCounters::UNCHARGED
                    },
                )?;
                bytes = bytes
                    .checked_add(path.retained_bytes())
                    .ok_or(CaptureError::from(WorkError::Overflow))?;
            }
        }
        meter
            .with_original_leaf(|work| work.claim_allocation(bytes, 0, WorkCounters::UNCHARGED))
            .map_err(capture_demand_error)?;
        let cancellation = meter.cancellation();
        if options.maximum_paths == 0
            || options.maximum_extent_spans == 0
            || incoming.0.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
        {
            return Err(CaptureError::InvalidOptions);
        }
        let source_root = open_source_root(options, meter)?;
        let receipt = CaptureReceipt::default();
        let mut mutations = Vec::new();
        let mut ordinary = Vec::new();
        let mut rename_records = Vec::new();
        let mut renamed_directories = Vec::new();

        for (index, change) in incoming.0.into_iter().enumerate() {
            let (mut path, mut destination, intent) = match change {
                WatchChange::Created(path) => (path, None, CaptureIntent::Replace),
                WatchChange::Modified(path) | WatchChange::Removed(path) => {
                    (path, None, CaptureIntent::Complete)
                }
                WatchChange::Arrived(path) => (path, None, CaptureIntent::Arrived),
                WatchChange::MetadataChanged(path) => (path, None, CaptureIntent::MetadataOnly),
                WatchChange::Renamed { from, to } => (from, Some(to), CaptureIntent::Complete),
            };
            if incoming.1.change_has_native_payload_custody(index) {
                let owned = clone_capture_path(&path, meter)?;
                drop(path);
                path = owned;
                if let Some(to) = destination.take() {
                    destination = Some(clone_capture_path(&to, meter)?);
                    drop(to);
                }
            }
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            let excluded = capture_excludes(policy, &path, meter)?;
            let (path, intent) = if let Some(to) = destination {
                match (excluded, capture_excludes(policy, &to, meter)?) {
                    (false, false) => {
                        let from = path;
                        cancellation.check()?;
                        let prior_source = capture_remove(&mut ordinary, &from, meter)?;
                        let prior_source = if let Some((key, value)) = prior_source {
                            capture_drop_path(key, meter)?;
                            Some(value)
                        } else {
                            None
                        };
                        if let Some((key, _)) = capture_remove(&mut ordinary, &to, meter)? {
                            capture_drop_path(key, meter)?;
                        }
                        let record = if let Some((key, record)) =
                            capture_remove(&mut rename_records, &from, meter)?
                        {
                            capture_drop_path(key, meter)?;
                            record
                        } else if matches!(prior_source, Some((CurrentRecord::Known(None), _))) {
                            None
                        } else {
                            let source = checkout
                                .lookup_no_follow(&from, meter, cancellation)
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
                            capture_reserve(&mut mutations, 1, meter)?;
                            mutations.push(AuthoredMutation::Rename {
                                source: clone_capture_path(&from, meter)?,
                                destination: clone_capture_path(&to, meter)?,
                                replace: true,
                            });
                            let destination = clone_capture_path(&to, meter)?;
                            capture_replace(&mut rename_records, destination, Some(record), meter)?;
                        } else {
                            let destination = clone_capture_path(&to, meter)?;
                            {
                                capture_replace(
                                    &mut ordinary,
                                    destination,
                                    (CurrentRecord::Lookup, CaptureIntent::Replace),
                                    meter,
                                )
                            }?;
                        }
                        if from == to {
                            capture_drop_path(from, meter)?;
                        } else {
                            capture_replace(&mut rename_records, from, None, meter)?;
                        }
                        if record.is_none()
                            && let Some((key, _)) = capture_remove(&mut rename_records, &to, meter)?
                        {
                            capture_drop_path(key, meter)?;
                        }
                        capture_drop_path(to, meter)?;
                        continue;
                    }
                    (true, false) => {
                        capture_drop_path(path, meter)?;
                        (to, CaptureIntent::Replace)
                    }
                    (false, true) => {
                        capture_drop_path(to, meter)?;
                        (path, CaptureIntent::Complete)
                    }
                    (true, true) => {
                        capture_drop_path(path, meter)?;
                        capture_drop_path(to, meter)?;
                        continue;
                    }
                }
            } else if excluded {
                capture_drop_path(path, meter)?;
                continue;
            } else {
                (path, intent)
            };
            let current = current_record(&rename_records, &path, meter)?;
            let entry = capture_entry(&mut ordinary, path, (current, intent), meter)?;
            match intent {
                CaptureIntent::Replace => *entry = (current, intent),
                CaptureIntent::Arrived if entry.1 != CaptureIntent::Replace => entry.1 = intent,
                CaptureIntent::Complete if entry.1 == CaptureIntent::MetadataOnly => {
                    entry.1 = intent;
                }
                _ => {}
            }
        }
        if !changes_storage_native {
            meter
                .clone()
                .release(changes_bytes)
                .map_err(capture_demand_error)?;
        }
        drop(incoming.1);

        expand_directory_hints(
            checkout,
            &mut ordinary,
            &rename_records,
            &source_root,
            options.maximum_paths,
            policy,
            meter,
        )
        .await?;

        // A watcher may report a nested file before any ancestor has been observed in this lazy checkout. Capture only the missing ancestors, rather than expanding their entire host subtrees as event roots.
        let maximum_paths = usize::try_from(options.maximum_paths).unwrap_or(usize::MAX);
        let mut renamed_paths = 0;
        for (_, record) in &rename_records {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            renamed_paths += usize::from(record.is_some());
        }
        let ancestors = watch_ancestor_candidates(
            ordinary
                .iter()
                .map(|(path, _)| path)
                .chain(
                    rename_records
                        .iter()
                        .filter_map(|(path, record)| record.map(|_| path)),
                )
                .chain(mutations.iter().filter_map(|mutation| match mutation {
                    AuthoredMutation::Rename { destination, .. } => Some(destination),
                    _ => None,
                })),
            &ordinary,
            &rename_records,
            policy,
            maximum_paths.saturating_sub(renamed_paths),
            checkout.volume_config().limits,
            meter,
        )?;
        let missing_ancestors = hydrate_watch_ancestors(
            checkout,
            &mut ordinary,
            ancestors,
            &source_root,
            epoch,
            maximum_paths.saturating_sub(renamed_paths),
            meter,
        )
        .await?;

        let mut lookup_paths = capture_vec(ordinary.len(), meter)?;
        for (path, (current, _)) in &ordinary {
            if matches!(current, CurrentRecord::Lookup) {
                lookup_paths.push(clone_capture_path(path, meter)?);
            }
        }
        if lookup_paths.is_empty() {
            release_capture_paths(lookup_paths, meter)?;
        } else {
            let lookup = checkout
                .lookup_batch_no_follow(&lookup_paths, meter, cancellation)
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
                        .lookup_no_follow(&prior_path, meter, cancellation)
                        .await?;
                    record = prior.record;
                    capture_drop_path(prior_path, meter)?;
                }
                let Ok(index) = capture_position(&ordinary, &path, meter)? else {
                    return Err(CaptureError::InvalidOptions);
                };
                let (current, _) = &mut ordinary.get_mut(index).ok_or(WorkError::Overflow)?.1;
                *current = CurrentRecord::Known(record);
                capture_drop_path(path, meter)?;
            }
            drop(lookup.1);
            meter
                .clone()
                .release(lookup_paths_bytes)
                .map_err(capture_demand_error)?;
        }
        {
            let storage_bytes = renamed_directories.capacity() as u64
                * std::mem::size_of::<(NamespacePath, NamespacePath)>() as u64;
            for (from, to) in renamed_directories {
                capture_drop_path(from, meter)?;
                capture_drop_path(to, meter)?;
            }
            meter
                .clone()
                .release(storage_bytes)
                .map_err(capture_demand_error)
        }?;

        // Hydration preserves ancestor depth order, so parents precede every rename.
        let rename_mutations = std::mem::take(&mut mutations);
        let mut batch = CaptureBatch {
            source_root: &source_root,
            detachment_base: checkout.root().file_table.digest,
            watch_epoch: Some(epoch),
            baseline: None,
            maximum_extent_spans: options.maximum_extent_spans,
            meter,
            host_links: Vec::new(),
            mutations,
            receipt,
        };
        let missing_bytes = vector_bytes(&missing_ancestors);
        for (path, current) in missing_ancestors {
            batch
                .prepare(checkout, (None, path, current), CaptureIntent::Replace)
                .await?;
        }
        meter
            .clone()
            .release(missing_bytes)
            .map_err(capture_demand_error)?;
        let rename_mutations_bytes =
            rename_mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
        capture_reserve(&mut batch.mutations, rename_mutations.len(), meter)?;
        {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Copy.delta(
                    rename_mutations.len() as u64 * std::mem::size_of::<AuthoredMutation>() as u64,
                ),
            )
        }?;
        batch.mutations.extend(rename_mutations);
        meter
            .clone()
            .release(rename_mutations_bytes)
            .map_err(capture_demand_error)?;
        let namespace_mutations = std::mem::take(&mut batch.mutations);
        let rename_records_bytes = vector_bytes(&rename_records);
        for (path, record) in rename_records {
            let Some(record) = record else {
                capture_drop_path(path, meter)?;
                continue;
            };
            if capture_position(&ordinary, &path, meter)?.is_ok() {
                capture_drop_path(path, meter)?;
                continue;
            }
            batch
                .prepare(
                    checkout,
                    (None, path, Some(record)),
                    CaptureIntent::Complete,
                )
                .await?;
        }
        meter
            .clone()
            .release(rename_records_bytes)
            .map_err(capture_demand_error)?;
        let mut observed_ordinary = capture_vec(ordinary.len(), meter)?;
        let ordinary_bytes = vector_bytes(&ordinary);
        for (path, (current, intent)) in ordinary {
            let CurrentRecord::Known(current) = current else {
                return Err(CaptureError::InvalidOptions);
            };
            let host_path = namespace_to_host_path_measured(&path, meter)?;
            let observation = observe_host_path(&source_root, &host_path, meter)?;
            capture_drop_host_path(host_path, meter)?;
            observed_ordinary.push((path, current, intent, observation));
        }
        meter
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
            batch
                .prepare(checkout, (observation, path, current), intent)
                .await?;
        }
        meter
            .clone()
            .release(observed_bytes)
            .map_err(capture_demand_error)?;
        release_host_links(batch.host_links, meter)?;
        apply_capture_transaction(checkout, namespace_mutations, meter).await?;
        apply_capture_transaction(checkout, batch.mutations, meter).await?;
        meter.clone().check().map_err(capture_demand_error)?;
        Ok(WatchCaptureReceipt {
            epoch,
            first_sequence,
            next_sequence,
            capture: batch.receipt,
        })
    }
    .await;
    let receipt = capture_scope_result(scope, result)?;
    cancellation
        .commit_if_live(|| *checkout = candidate)
        .map_err(|error| OperationFailure::new(error.into(), native.receipt()))?;
    Ok(receipt)
}
