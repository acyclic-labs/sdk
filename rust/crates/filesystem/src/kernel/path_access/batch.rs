#![allow(
    clippy::indexing_slicing,
    reason = "Query indices are enumerated once; parallel vectors share their length and cursor loops check bounds."
)]
use super::*;

pub(super) fn active_order(
    left: &ActivePath,
    right: &ActivePath,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Ordering, crate::FsError> {
    examine_item(accounting, None, || {
        compare_object_id(&left.directory, &right.directory)
            .then_with(|| left.path_index.cmp(&right.path_index))
    })
}
pub(super) fn sift_active(
    values: &mut [ActivePath],
    mut root: usize,
    end: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    loop {
        let Some(mut child) = root
            .checked_mul(2)
            .and_then(|index| index.checked_add(1))
            .filter(|index| *index < end)
        else {
            return Ok(());
        };
        if child + 1 < end
            && active_order(&values[child], &values[child + 1], accounting)? == Ordering::Less
        {
            child += 1;
        }
        if active_order(&values[root], &values[child], accounting)? != Ordering::Less {
            return Ok(());
        }
        values.swap(root, child);
        root = child;
    }
}
pub(super) fn sort_active(
    values: &mut [ActivePath],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    for root in (0..values.len() / 2).rev() {
        sift_active(values, root, values.len(), accounting)?;
    }
    for end in (1..values.len()).rev() {
        values.swap(0, end);
        sift_active(values, 0, end, accounting)?;
    }
    Ok(())
}
#[derive(Clone, Copy)]
pub(super) struct PendingBinding {
    path_index: usize,
    file_id: crate::foundation::FileId,
    kind: FileKind,
}
#[derive(Clone, Copy)]
pub(super) struct ActivePath {
    path_index: usize,
    directory: ObjectId,
}
#[derive(Clone, Copy)]
pub(crate) enum PathQueries<'a> {
    Owned(&'a [NamespacePath]),
    Borrowed(&'a [&'a NamespacePath]),
}
impl<'a> PathQueries<'a> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Owned(paths) => paths.len(),
            Self::Borrowed(paths) => paths.len(),
        }
    }
    pub(super) fn get(self, index: usize) -> &'a NamespacePath {
        match self {
            Self::Owned(paths) => &paths[index],
            Self::Borrowed(paths) => paths[index],
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
pub(super) async fn path_queries_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: PathQueries<'_>,
    observe: bool,
    capture_terminal: bool,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    mode: PathBatchMode,
) -> Result<(ObservedPathBatch, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    config.validate()?;
    if paths.len() == 0 {
        return Err(PathLookupError::EmptyBatch.into());
    }
    if u32::try_from(paths.len()).unwrap_or(u32::MAX) > config.limits.maximum_paths_per_batch {
        return Err(PathLookupError::TooManyPaths.into());
    }
    let mut total_components = 0_usize;
    for index in 0..paths.len() {
        validate_path(paths.get(index), config, accounting)?;
        total_components = total_components
            .checked_add(paths.get(index).depth())
            .ok_or(WorkError::Overflow)?;
    }
    let cache_entries = maximum_cache_entries_for_components(total_components, config)
        .map_err(|failure| crate::FsError::Path(failure.error))?
        .min(4096);
    let initial_live = standalone_live(accounting)?;
    let result = {
        let mut frame_guard = None;
        let mut frame_observation = FrameObservation {
            owner: data_custody,
            bytes: 0,
        };
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (&mut *accounting, store, generation, paths, data_custody),
            |(accounting, ..), bytes| {
                frame_guard = Some(admit_path_frame(accounting, bytes, &mut frame_observation)?);
                Ok::<(), crate::FsError>(())
            },
            |(accounting, store, generation, paths, data_custody)| async move {
                let cache =
                    OperationReadCache::new(store, cache_entries, accounting, data_custody)?;
                let count = paths.len();
                let limits = DecodeLimits::for_volume(config);
                let mut dependencies = reserve_slots::<Dependency>(
                    if observe {
                        total_components
                            .checked_add(count)
                            .ok_or(WorkError::Overflow)?
                    } else {
                        0
                    },
                    accounting,
                    data_custody,
                )?;
                let mut entries = reserve_slots::<PathBatchEntry>(count, accounting, data_custody)?;
                let mut current =
                    reserve_slots::<Option<FileRecord>>(count, accounting, data_custody)?;
                let mut done = reserve_slots::<u8>(count, accounting, data_custody)?;
                let mut active = reserve_slots::<ActivePath>(count, accounting, data_custody)?;
                let mut names = reserve_slots::<LogicalName>(count, accounting, data_custody)?;
                let mut query_indices = reserve_slots::<usize>(count, accounting, data_custody)?;
                let mut pending = reserve_slots::<PendingBinding>(count, accounting, data_custody)?;
                let mut file_ids =
                    reserve_slots::<crate::foundation::FileId>(count, accounting, data_custody)?;
                entries.value.resize(
                    count,
                    PathBatchEntry {
                        record: None,
                        parent: None,
                        resolved_components: 0,
                    },
                );
                current.value.resize(count, None);
                done.value.resize(count, 0);
                let root = {
                    let (root, _root_guard) =
                        crate::kernel::file_table::lookup_file_record_async_with_accounting(
                            &cache,
                            generation.file_table,
                            generation.root_file_id,
                            limits,
                            accounting,
                            cancellation,
                            data_custody,
                        )
                        .await?;
                    cache.reconcile(accounting)?;
                    root.ok_or(PathLookupError::MissingRootRecord)?
                };
                for index in 0..count {
                    let is_root = examine_item(accounting, None, || paths.get(index).is_root())?;
                    if is_root {
                        entries.value[index].record = Some(root);
                        done.value[index] = 1;
                        if observe && capture_terminal {
                            let expected =
                                crate::kernel::probe::capture_file_record_state_with_accounting(
                                    root,
                                    accounting,
                                    Some(cancellation),
                                    data_custody,
                                )?;
                            dependencies.value.push(Dependency {
                                region: DependencyRegion::FileRecord(root.file_id),
                                expected,
                            });
                        }
                    } else {
                        current.value[index] = Some(root);
                    }
                }
                let maximum_depth = (0..count)
                    .map(|index| paths.get(index).depth())
                    .max()
                    .unwrap_or(0);
                for depth in 0..maximum_depth {
                    pending.value.clear();
                    active.value.clear();
                    for index in 0..count {
                        let finished = examine_item(accounting, None, || {
                            done.value[index] != 0 || paths.get(index).depth() <= depth
                        })?;
                        if finished {
                            continue;
                        }
                        let FilePayload::Directory { entries: directory } = current.value[index]
                            .ok_or(PathLookupError::KindMismatch)?
                            .payload
                        else {
                            if matches!(mode, PathBatchMode::SelectedRestore) {
                                entries.value[index] = PathBatchEntry {
                                    record: None,
                                    parent: None,
                                    resolved_components: u16::try_from(depth).unwrap_or(u16::MAX),
                                };
                                done.value[index] = 1;
                                continue;
                            }
                            return Err(PathLookupError::NotDirectory.into());
                        };
                        active.value.push(ActivePath {
                            path_index: index,
                            directory,
                        });
                    }
                    sort_active(&mut active.value, accounting)?;
                    let mut cursor = 0;
                    while cursor < active.value.len() {
                        let directory = active.value[cursor].directory;
                        let start = cursor;
                        cursor += 1;
                        while cursor < active.value.len() {
                            let same_directory = examine_item(accounting, None, || {
                                active.value[cursor].directory == directory
                            })?;
                            if !same_directory {
                                break;
                            }
                            cursor += 1;
                        }
                        names.value.clear();
                        query_indices.value.clear();
                        let mut copied_bytes = 0_u64;
                        for item in &active.value[start..cursor] {
                            query_indices.value.push(item.path_index);
                            let (name, guard) = copy_name(
                                &paths.get(item.path_index).components()[depth],
                                limits.maximum_name_bytes,
                                accounting,
                                data_custody,
                            )?;
                            let copied = crate::kernel::file_read::ReadOwned {
                                value: name,
                                custody: guard,
                            };
                            copied_bytes = copied_bytes
                                .checked_add(usize_to_u64(copied.value.retained_bytes()))
                                .ok_or(WorkError::Overflow)?;
                            let crate::kernel::file_read::ReadOwned {
                                value: name,
                                custody: mut guard,
                            } = copied;
                            let owner = if observe {
                                &mut dependencies.custody
                            } else {
                                &mut names.custody
                            };
                            if let Err(error) = absorb_allocation(owner, &mut guard) {
                                drop(name);
                                return Err(error);
                            }
                            names.value.push(name);
                        }
                        let (bindings, bindings_guard) =
                            crate::kernel::read::lookup_tree_entries_async_with_accounting(
                                &cache,
                                directory,
                                &names.value,
                                config.limits.maximum_paths_per_batch,
                                limits,
                                accounting,
                                cancellation,
                                data_custody,
                            )
                            .await?;
                        let bindings = ReadOwned {
                            value: bindings,
                            custody: bindings_guard,
                        };
                        cache.reconcile(accounting)?;
                        for ((index, binding), name) in query_indices
                            .value
                            .iter()
                            .zip(bindings.value.iter())
                            .zip(names.value.drain(..))
                        {
                            if observe {
                                let expected = match binding {
                                Some(binding) => {
                                    crate::kernel::probe::capture_directory_name_state_with_accounting(
                                        binding,
                                        accounting,
                                        Some(cancellation),
                                        data_custody,
                                    )?
                                }
                                None => DependencyState::Absent,
                            };
                                dependencies.value.push(Dependency {
                                    region: DependencyRegion::DirectoryName {
                                        directory_id: current.value[*index]
                                            .ok_or(PathLookupError::KindMismatch)?
                                            .file_id,
                                        name,
                                    },
                                    expected,
                                });
                            }
                            if let Some(binding) = binding {
                                pending.value.push(PendingBinding {
                                    path_index: *index,
                                    file_id: binding.file_id,
                                    kind: binding.kind,
                                });
                            } else {
                                entries.value[*index] = PathBatchEntry {
                                    record: None,
                                    parent: current.value[*index],
                                    resolved_components: u16::try_from(depth).unwrap_or(u16::MAX),
                                };
                                done.value[*index] = 1;
                            }
                        }
                        drop_bindings(bindings, accounting)?;
                        if !observe {
                            retire_name_bytes(&mut names.custody, accounting, copied_bytes)?;
                        }
                    }
                    if pending.value.is_empty() {
                        continue;
                    }
                    file_ids.value.clear();
                    file_ids
                        .value
                        .extend(pending.value.iter().map(|binding| binding.file_id));
                    let (records, records_guard) =
                        crate::kernel::file_table::lookup_file_records_async_with_accounting(
                            &cache,
                            generation.file_table,
                            &file_ids.value,
                            config.limits.maximum_paths_per_batch,
                            limits,
                            accounting,
                            cancellation,
                            data_custody,
                        )
                        .await?;
                    let records = ReadOwned {
                        value: records,
                        custody: records_guard,
                    };
                    cache.reconcile(accounting)?;
                    for (binding, record) in pending.value.iter().zip(records.value.iter().copied())
                    {
                        let record = record.ok_or(PathLookupError::KindMismatch)?;
                        if record.kind != binding.kind {
                            return Err(PathLookupError::KindMismatch.into());
                        }
                        let next_depth = depth + 1;
                        if next_depth == paths.get(binding.path_index).depth() {
                            if observe && capture_terminal {
                                let expected =
                                crate::kernel::probe::capture_file_record_state_with_accounting(
                                    record,
                                    accounting,
                                    Some(cancellation),
                                    data_custody,
                                )?;
                                dependencies.value.push(Dependency {
                                    region: DependencyRegion::FileRecord(record.file_id),
                                    expected,
                                });
                            }
                            entries.value[binding.path_index] = PathBatchEntry {
                                record: Some(record),
                                parent: current.value[binding.path_index],
                                resolved_components: u16::try_from(next_depth).unwrap_or(u16::MAX),
                            };
                            done.value[binding.path_index] = 1;
                            current.value[binding.path_index] = None;
                        } else {
                            current.value[binding.path_index] = Some(record);
                        }
                    }
                    drop_slots(records, accounting)?;
                }
                if done.value.contains(&0) {
                    return Err(PathLookupError::KindMismatch.into());
                }
                let retained_allocation_bytes = usize_to_u64(entries.value.capacity())
                    .checked_mul(size_of::<PathBatchEntry>() as u64)
                    .ok_or(WorkError::Overflow)?;
                // Scratch dies before guards; output arrays keep their exact guards.
                drop_slots(file_ids, accounting)?;
                drop_slots(pending, accounting)?;
                drop_slots(query_indices, accounting)?;
                drop_slots(names, accounting)?;
                drop_slots(active, accounting)?;
                drop_slots(done, accounting)?;
                drop_slots(current, accounting)?;
                let ReadOwned {
                    value: dependencies,
                    custody: mut allocation,
                } = dependencies;
                let ReadOwned {
                    value: entries,
                    custody: mut entries_guard,
                } = entries;
                if let Err(error) = absorb_allocation(&mut allocation, &mut entries_guard) {
                    drop(entries);
                    drop(dependencies);
                    return Err(error);
                }
                let result = (
                    ObservedPathBatch {
                        lookup: PathBatchLookup {
                            entries,
                            retained_allocation_bytes,
                            work: accounting.spent(),
                        },
                        dependencies,
                    },
                    allocation,
                );
                drop_cache(cache, accounting)?;
                Ok(result)
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_observation);
        result
    };
    if initial_live.is_some() {
        let retained = match &result {
            Ok(result) => result
                .0
                .lookup
                .retained_allocation_bytes
                .checked_add(dependency_bytes(
                    &result.0.dependencies,
                    result.0.dependencies.capacity(),
                )?)
                .ok_or(WorkError::Overflow)?,
            Err(_) => 0,
        };
        cleanup_standalone(accounting, initial_live, retained)?;
    }
    result.map(|mut result| {
        result.0.lookup.work = accounting.spent();
        result
    })
}
