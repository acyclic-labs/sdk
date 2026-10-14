use super::*;

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
pub(super) async fn resolve_path_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    observe: bool,
    capture_terminal: bool,
) -> Result<(ObservedPathLookup, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    validate_path(path, config, accounting)?;
    let cache_entries = maximum_cache_entries_for_components(path.depth(), config)
        .map_err(|failure| crate::FsError::Path(failure.error))?;
    let initial_live = standalone_live(accounting)?;
    let mut frame_guard = None;
    let mut frame_observation = FrameObservation {
        owner: data_custody,
        bytes: 0,
    };
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, store, generation, path, data_custody),
        |(accounting, ..), bytes| {
            frame_guard = Some(admit_path_frame(accounting, bytes, &mut frame_observation)?);
            Ok::<(), crate::FsError>(())
        },
        |(accounting, store, generation, path, data_custody)| async move {
            let limits = DecodeLimits::for_volume(config);
            let cache = OperationReadCache::new(store, cache_entries, accounting, data_custody)?;
            let mut dependencies = reserve_slots::<Dependency>(
                if observe {
                    path.depth().saturating_add(1)
                } else {
                    0
                },
                accounting,
                data_custody,
            )?;
            let root = crate::kernel::file_table::lookup_file_record_async_with_accounting(
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
            let (root_record, root_guard) = root;
            let mut current = root_record.ok_or(PathLookupError::MissingRootRecord)?;
            drop(root_guard);
            let mut parent = None;
            let mut resolved = 0;
            let mut found = true;
            for (index, component) in path.components().iter().enumerate() {
                let payload = examine_item(accounting, None, || current.payload)?;
                parent = Some(current);
                let FilePayload::Directory { entries } = payload else {
                    if observe && !capture_terminal {
                        found = false;
                        break;
                    }
                    return Err(PathLookupError::NotDirectory.into());
                };
                if current.kind != FileKind::Directory {
                    return Err(PathLookupError::KindMismatch.into());
                }
                let copied = if observe {
                    let (name, guard) = copy_name(
                        component,
                        limits.maximum_name_bytes,
                        accounting,
                        data_custody,
                    )?;
                    Some(crate::kernel::file_read::ReadOwned {
                        value: name,
                        custody: guard,
                    })
                } else {
                    None
                };
                let (entry, entry_guard) =
                    crate::kernel::read::lookup_tree_entry_async_with_accounting(
                        &cache,
                        entries,
                        component,
                        limits,
                        accounting,
                        cancellation,
                        data_custody,
                    )
                    .await?;
                let entry = crate::kernel::file_read::ReadOwned {
                    value: entry,
                    custody: entry_guard,
                };
                cache.reconcile(accounting)?;
                if let Some(copied) = copied {
                    let expected = match &entry.value {
                        Some(entry) => {
                            crate::kernel::probe::capture_directory_name_state_with_accounting(
                                entry,
                                accounting,
                                Some(cancellation),
                                data_custody,
                            )?
                        }
                        None => DependencyState::Absent,
                    };
                    let crate::kernel::file_read::ReadOwned {
                        value: name,
                        custody: mut guard,
                    } = copied;
                    // Original guards combine without copying data or charging again.
                    if let Err(error) = absorb_allocation(&mut dependencies.custody, &mut guard) {
                        drop(name);
                        return Err(error);
                    }
                    dependencies.value.push(Dependency {
                        region: DependencyRegion::DirectoryName {
                            directory_id: current.file_id,
                            name,
                        },
                        expected,
                    });
                }
                let Some(binding) = entry.value.as_ref() else {
                    found = false;
                    resolved = index;
                    drop(entry);
                    break;
                };
                let file_id = binding.file_id;
                let kind = binding.kind;
                let entry_bytes = match &entry.custody {
                    KernelOwnedAllocation::Standalone => {
                        Some(usize_to_u64(binding.name.retained_bytes()))
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    KernelOwnedAllocation::NativeOriginal(_) => None,
                };
                drop(entry);
                if let Some(bytes) = entry_bytes {
                    release_standalone(accounting, bytes)?;
                }
                let (record, record_guard) =
                    crate::kernel::file_table::lookup_file_record_async_with_accounting(
                        &cache,
                        generation.file_table,
                        file_id,
                        limits,
                        accounting,
                        cancellation,
                        data_custody,
                    )
                    .await?;
                cache.reconcile(accounting)?;
                current = record.ok_or(PathLookupError::KindMismatch)?;
                drop(record_guard);
                if current.kind != kind {
                    return Err(PathLookupError::KindMismatch.into());
                }
                resolved = index + 1;
            }
            if observe && found && capture_terminal {
                let expected = crate::kernel::probe::capture_file_record_state_with_accounting(
                    current,
                    accounting,
                    Some(cancellation),
                    data_custody,
                )?;
                dependencies.value.push(Dependency {
                    region: DependencyRegion::FileRecord(current.file_id),
                    expected,
                });
            }
            // Move result buffers first, then move their unchanged original guards.
            let ReadOwned {
                value: dependencies,
                custody: allocation,
            } = dependencies;
            let result = (
                ObservedPathLookup {
                    lookup: PathLookup {
                        record: found.then_some(current),
                        parent,
                        resolved_components: u16::try_from(resolved).unwrap_or(u16::MAX),
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
    drop(frame_guard);
    if initial_live.is_some() {
        let retained = match &result {
            Ok(result) => {
                dependency_bytes(&result.0.dependencies, result.0.dependencies.capacity())?
            }
            Err(_) => 0,
        };
        cleanup_standalone(accounting, initial_live, retained)?;
    }
    result.map(|mut result| {
        result.0.lookup.work = accounting.spent();
        result
    })
}
