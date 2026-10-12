use super::*;

/// Explicitly materializes one checkout without changing or publishing it.
pub async fn materialize_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)
        .map_err(|_| OperationFailure::before_work(MaterializeError::InvalidPath))?;
    materialize_checkout_paths(
        checkout,
        std::slice::from_ref(&root),
        options,
        budget,
        cancellation,
    )
    .await
}

pub(super) async fn materialize_heap_future_on_receipt<F, E>(
    create: impl FnOnce(GenerationReceipt) -> F,
    mut receipt: GenerationReceipt,
) -> Result<OperationReceipt<MaterializationReceipt>, (E, WorkCounters)>
where
    F: Future<Output = Result<OperationReceipt<MaterializationReceipt>, E>>,
    E: From<MaterializeError>,
{
    // Retire the stack-safety box before releasing its allocation.
    receipt
        .custody
        .grow((std::mem::size_of::<F>()) as u64)
        .map_err(materialize_native_error)
        .map_err(|error| (E::from(error), receipt.custody.work().receipt()))?;
    let custody = receipt
        .custody
        .split_off((std::mem::size_of::<F>()) as u64)
        .map_err(materialize_native_error)
        .map_err(|error| (E::from(error), receipt.custody.work().receipt()))?;
    let meter = receipt.custody.work().clone();
    let mut future = in_heap(move || create(receipt));
    let result = future.as_mut().await;
    drop(future);
    drop(custody);
    let work = meter.lock().receipt();
    match result {
        Ok(mut receipt) => {
            receipt.work = work;
            Ok(receipt)
        }
        Err(error) => Err((error, work)),
    }
}

/// Materializes multiple non-overlapping authenticated paths in one traversal.
pub async fn materialize_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    validate_bounds(options).map_err(OperationFailure::before_work)?;
    if paths.is_empty() || (paths.len() != 1 && paths.iter().any(|path| path.is_root())) {
        return Err(OperationFailure::before_work(
            MaterializeError::InvalidOptions,
        ));
    }
    let receipt = GenerationReceipt::new(budget, cancellation)
        .map_err(|(error, work)| OperationFailure::new(error, work))?;
    materialize_heap_future_on_receipt(
        move |receipt| {
            materialize_source(
                GenerationSource::Standalone(checkout),
                paths,
                options,
                MaterializeMode::DurableOutput,
                receipt,
                None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>,
            )
        },
        receipt,
    )
    .await
    .map_err(|(error, work)| OperationFailure::new(error, work))
}

pub(super) enum GenerationSource<'a, A, O> {
    Standalone(&'a mut Checkout<A, O>),
    Original(&'a PinnedReader<A, O>),
}

/// Materializes an original-native immutable reader on its existing ledger.
pub async fn materialize_checkout_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    options: &MaterializeOptions,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<MaterializationReceipt>, MaterializeError> {
    let receipt = GenerationReceipt::from_work(work)?;
    let validated = reader.validate_native_work(receipt.custody.work());
    validated.map_err(MaterializeError::from)?;
    materialize_heap_future_on_receipt(
        move |receipt| async move {
            let root = NamespacePath::new(Vec::new(), reader.volume_config().limits)
                .map_err(|_| MaterializeError::InvalidPath)?;
            validate_bounds(options)?;
            materialize_source(
                GenerationSource::Original(reader),
                std::slice::from_ref(&root),
                options,
                MaterializeMode::DurableOutput,
                receipt,
                None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>,
            )
            .await
        },
        receipt,
    )
    .await
    .map_err(|failure| failure.0)
}

pub(crate) async fn materialize_reader_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    mode: MaterializeMode,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<MaterializationReceipt>, MaterializeError> {
    let receipt = GenerationReceipt::from_work(work)?;
    let validated = reader.validate_native_work(receipt.custody.work());
    validated.map_err(MaterializeError::from)?;
    validate_bounds(options)?;
    if paths.is_empty()
        || (paths.len() != 1 && paths.iter().any(|path| path.components().is_empty()))
    {
        return Err(MaterializeError::InvalidOptions);
    }
    materialize_source(
        GenerationSource::Original(reader),
        paths,
        options,
        mode,
        receipt,
        None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one canonical traversal dispatches ordinary or original-native source leaves"
)]
pub(super) async fn materialize_source<A, O, E, F, Fut>(
    mut source: GenerationSource<'_, A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    mode: MaterializeMode,
    mut receipt: GenerationReceipt,
    mut before_effect: Option<&mut F>,
) -> Result<OperationReceipt<MaterializationReceipt>, E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let cancellation = receipt
        .custody
        .work()
        .state
        .with_original_leaf(|original| original.cancellation().clone());
    let cancellation = &cancellation;
    let reader = match &source {
        GenerationSource::Standalone(checkout) => {
            let reader = checkout
                .pinned_reader_for_materialization_with_work(receipt.custody.work(), cancellation);
            reader.map_err(MaterializeError::from)?
        }
        GenerationSource::Original(reader) => {
            reader
                .validate_native_work(receipt.custody.work())
                .map_err(MaterializeError::from)?;
            (*reader).clone()
        }
    };
    let mut selected_paths = Vec::<&NamespacePath>::new();
    receipt.reserve(&mut selected_paths, paths.len())?;
    for path in paths {
        let mut insertion = selected_paths.len();
        while insertion != 0 {
            let previous = selected_paths[insertion - 1];
            receipt.run(|work| {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
            })?;
            if previous <= path {
                break;
            }
            insertion -= 1;
        }
        receipt.run(|work| {
            work.charge(WorkCounters {
                bytes_copied: ((selected_paths.len() - insertion + 1)
                    * std::mem::size_of::<&NamespacePath>()) as u64,
                ..WorkCounters::UNCHARGED
            })
        })?;
        selected_paths.insert(insertion, path);
    }
    for pair in selected_paths.windows(2) {
        receipt.run(|work| {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
        })?;
        if matches!(pair, [ancestor, path] if path.is_within(ancestor)) {
            return Err(MaterializeError::OverlappingPaths.into());
        }
    }
    let selection_bytes = vec_bytes(&selected_paths);
    let destination = validate_options(options, &mut receipt)?;
    let host_root = receipt.owned_arc(destination, 0)?;
    let mut known_files = Vec::<(FileId, Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>)>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = Vec::<(
        (u64, ObjectId, ObjectId),
        Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    )>::new();
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    let limits = reader.volume_config().limits;
    for path in selected_paths {
        let host_path = materialize_host_path(None, path.components(), &mut receipt)?;
        if path.is_root() {
            let namespace = clone_namespace_measured(path, None, limits, &mut receipt)?;
            let bytes = namespace.retained_bytes() as usize;
            let namespace = receipt.owned_arc(namespace, bytes)?;
            receipt.push(&mut pending, (namespace.clone(), host_path.clone()))?;
            receipt.push(&mut deferred_directory_metadata, (namespace, host_path))?;
            continue;
        }
        let record = match &mut source {
            GenerationSource::Standalone(checkout) => {
                let remaining = receipt.run(|work| work.remaining())?;
                let lookup = checkout
                    .lookup_no_follow(path, remaining, cancellation)
                    .await
                    .map_err(|failure| receipt.engine_failure(failure))?;
                receipt.run(|work| work.merge(lookup.work))?;
                lookup.value.record
            }
            GenerationSource::Original(reader) => {
                let lookup = reader
                    .lookup_record_with_native_work(path, receipt.custody.work(), cancellation)
                    .await;
                lookup.map_err(MaterializeError::from)?
            }
        }
        .ok_or(MaterializeError::MissingPath)?;
        generation_action(&mut before_effect, NativeMaterializationActionPhase::Before).await?;
        receipt.run(|work| {
            #[cfg(unix)]
            if let Some(parent) = super::work::host_path(&host_path.0)
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
            {
                host_root.0.create_dir_all_held(parent, work)?;
            }
            #[cfg(windows)]
            if let Some(separator) = host_path
                .0
                .iter()
                .rposition(|unit| *unit == u16::from(b'\\'))
            {
                host_root
                    .0
                    .create_dir_all_held_wide(&host_path.0[..separator], work)?;
            }
            Ok(())
        })?;
        generation_action(
            &mut before_effect,
            NativeMaterializationActionPhase::Completed,
        )
        .await?;
        let directory = if record.kind == FileKind::Directory {
            let namespace = clone_namespace_measured(path, None, limits, &mut receipt)?;
            let bytes = namespace.retained_bytes() as usize;
            let namespace = receipt.owned_arc(namespace, bytes)?;
            receipt.push(
                &mut deferred_directory_metadata,
                (namespace.clone(), host_path.clone()),
            )?;
            Some(namespace)
        } else {
            None
        };
        materialize_entry(
            &reader,
            &host_root,
            path,
            &host_path,
            directory,
            record.file_id,
            record.kind,
            record.payload,
            #[cfg(any(target_os = "macos", windows))]
            record.metadata,
            options,
            cancellation,
            &mut known_files,
            #[cfg(any(target_os = "macos", windows))]
            &mut known_payloads,
            &mut pending,
            &mut receipt,
            mode,
            &mut before_effect,
        )
        .await?;
    }
    receipt
        .custody
        .release(selection_bytes as u64)
        .map_err(materialize_native_error)?;
    while let Some((directory, host_directory)) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let page = match &mut source {
                GenerationSource::Standalone(checkout) => {
                    let remaining = receipt.run(|work| work.remaining())?;
                    let page = checkout
                        .list_directory_records(
                            &directory.0,
                            after
                                .as_ref()
                                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            options.maximum_directory_entries,
                            remaining,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| receipt.engine_failure(failure))?;
                    receipt.run(|work| work.merge(page.work))?;
                    page.value
                }
                GenerationSource::Original(reader) => {
                    let page = reader
                        .list_directory_records(
                            &directory.0,
                            after.as_ref(),
                            options.maximum_directory_entries,
                            receipt.custody.work(),
                            cancellation,
                        )
                        .await;
                    let (page, custody) = page.map_err(MaterializeError::from)?;
                    receipt.accept_source(page, custody)?
                }
            };
            if page.has_more && page.entries.is_empty() {
                return Err(
                    MaterializeError::Engine("directory pagination made no progress").into(),
                );
            }
            let entry_bytes = vec_bytes(&page.entries);
            if matches!(&source, GenerationSource::Standalone(_)) {
                receipt.adopt(entry_bytes)?;
                for entry in &page.entries {
                    receipt.adopt(entry.name.retained_bytes())?;
                }
            }
            for entry in page.entries {
                let child = clone_namespace_measured(
                    &directory.0,
                    Some(&entry.name),
                    limits,
                    &mut receipt,
                )?;
                let child_host = materialize_host_path(
                    Some(&host_directory.0),
                    std::slice::from_ref(&entry.name),
                    &mut receipt,
                )?;
                let child_bytes = child.retained_bytes() as usize;
                let mut owned_child = Some(child);
                let selected_directory = if entry.record.kind == FileKind::Directory {
                    let child = receipt.owned_arc(
                        owned_child.take().ok_or(MaterializeError::InvalidOptions)?,
                        child_bytes,
                    )?;
                    receipt.push(
                        &mut deferred_directory_metadata,
                        (child.clone(), child_host.clone()),
                    )?;
                    Some(child)
                } else {
                    None
                };
                let child = selected_directory
                    .as_deref()
                    .map(|owned| &owned.0)
                    .or(owned_child.as_ref())
                    .ok_or(MaterializeError::InvalidOptions)?;
                materialize_entry(
                    &reader,
                    &host_root,
                    child,
                    &child_host,
                    selected_directory.clone(),
                    entry.record.file_id,
                    entry.record.kind,
                    entry.record.payload,
                    #[cfg(any(target_os = "macos", windows))]
                    entry.record.metadata,
                    options,
                    cancellation,
                    &mut known_files,
                    #[cfg(any(target_os = "macos", windows))]
                    &mut known_payloads,
                    &mut pending,
                    &mut receipt,
                    mode,
                    &mut before_effect,
                )
                .await?;
                if entry.record.kind != FileKind::Directory {
                    drop(owned_child);
                    receipt
                        .custody
                        .release(child_bytes as u64)
                        .map_err(materialize_native_error)?;
                }
                if let Some(previous) = after.replace(entry.name) {
                    let bytes = previous.retained_bytes();
                    drop(previous);
                    receipt
                        .custody
                        .release(bytes as u64)
                        .map_err(materialize_native_error)?;
                }
            }
            receipt
                .custody
                .release(entry_bytes as u64)
                .map_err(materialize_native_error)?;
            if !page.has_more {
                break;
            }
        }
        if let Some(after) = after {
            let bytes = after.retained_bytes();
            drop(after);
            receipt
                .custody
                .release(bytes as u64)
                .map_err(materialize_native_error)?;
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            &reader,
            &host_root,
            &directory.0,
            &host_directory,
            None,
            cancellation,
            &mut receipt,
            &mut before_effect,
        )
        .await?;
    }
    let work = receipt.custody.work().receipt();
    Ok(OperationReceipt {
        value: receipt.value,
        work,
    })
}

/// Keeps all host-relative selections in one original traversal and hard-link map.
pub(crate) async fn materialize_checkout_host_paths_with_work_guarded<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    relatives: &[String],
    options: &MaterializeOptions,
    work: &crate::NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<OperationReceipt<MaterializationReceipt>, E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let receipt = GenerationReceipt::from_work(work).map_err(E::from)?;
    let validated = reader.validate_native_work(receipt.custody.work());
    validated.map_err(MaterializeError::from).map_err(E::from)?;
    materialize_heap_future_on_receipt(
        move |mut receipt| async move {
            validate_bounds(options)?;
            let mut paths = Vec::new();
            receipt.reserve(&mut paths, relatives.len())?;
            let vector_bytes = vec_bytes(&paths);
            let mut paths = (
                paths,
                receipt
                    .custody
                    .split_off(vector_bytes as u64)
                    .map_err(materialize_native_error)?,
            );
            for relative in relatives {
                let mut path = generation_namespace_from_host(
                    Path::new(relative),
                    reader.volume_config(),
                    &mut receipt,
                )?;
                paths
                    .1
                    .absorb(&mut path.1)
                    .map_err(|error| MaterializeError::NativeWork(error))?;
                paths.0.push(path.0);
            }
            materialize_source(
                GenerationSource::Original(reader),
                &paths.0,
                options,
                MaterializeMode::DurableOutput,
                receipt,
                Some(before_effect),
            )
            .await
        },
        receipt,
    )
    .await
    .map_err(|failure| failure.0)
}

pub(super) fn generation_namespace_from_host(
    relative: &Path,
    config: crate::VolumeConfig,
    receipt: &mut GenerationReceipt,
) -> Result<(NamespacePath, crate::NativeAllocation), MaterializeError> {
    let limits = config.limits;
    let mut count = 0_usize;
    for component in relative.components() {
        receipt.run(|work| {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
        })?;
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(MaterializeError::InvalidPath);
        }
        count += 1;
        if count > usize::from(limits.maximum_path_depth) {
            return Err(MaterializeError::InvalidPath);
        }
    }
    if count == 0 {
        return Err(MaterializeError::InvalidPath);
    }
    let mut names = Vec::new();
    receipt.reserve(&mut names, count)?;
    for component in relative.components() {
        receipt.run(|work| {
            work.charge(WorkCounters {
                items_examined: 1,
                source_path_components: 1,
                ..WorkCounters::UNCHARGED
            })
        })?;
        let encoded = crate::native_name::host_name_bytes_with_native_work(
            component.as_os_str(),
            config.profile,
            limits.maximum_component_bytes,
            receipt.custody.work(),
        );
        let ((encoding, bytes), custody) = encoded.map_err(materialize_native_error)?;
        let bytes = receipt.accept_source(bytes, custody)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| MaterializeError::InvalidPath)?;
        names.push(logical);
    }
    let path = NamespacePath::new(names, limits).map_err(|_| MaterializeError::InvalidPath)?;
    let retained = path.retained_bytes() as usize;
    Ok((
        path,
        receipt
            .custody
            .split_off(retained as u64)
            .map_err(materialize_native_error)?,
    ))
}

/// Restores one authenticated host-relative path without touching siblings.
pub async fn restore_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<HostPathRestore>, OperationFailure<MaterializeError>> {
    if !budget.has_finite_allowances() {
        return Err(OperationFailure::before_work(
            MaterializeError::InvalidOptions,
        ));
    }
    let native_work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| OperationFailure::new(materialize_native_error(error), work))?;
    let result = async {
        validate_bounds(options)?;
        let allocation_work = native_work.clone();
        let cleanup_allocation = allocation_work
            .claim(
                std::mem::size_of::<MaterializeError>() as u64,
                u64::from((std::mem::size_of::<MaterializeError>() as u64) != 0),
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(materialize_native_error)?;
        let cleanup_slot = (
            Box::new(MaterializeError::InvalidOptions),
            cleanup_allocation,
        );
        let (destination_root, relative) = {
            let mut work = crate::NativeMaterializationWork::clone(&native_work);
            validate_host_relative_measured(relative, &mut work)?;
            (
                allocation_work
                    .path(&options.destination, None)
                    .map_err(materialize_native_error)?,
                allocation_work
                    .path(relative, None)
                    .map_err(materialize_native_error)?,
            )
        };
        let expected_root_identity = options.expected_root_identity;
        let prepared = host_io(None, native_work.clone(), move |work| {
            prepare_restore(
                destination_root,
                relative,
                expected_root_identity,
                replacement,
                work,
            )
        })
        .await
        .map_err(|error| {
            materialize_native_error(crate::NativeTreeMaterializationError::from_runtime_error(
                error,
            ))
        })??;
        let PreparedRestore {
            root,
            destination_parent,
            stage_owner,
            stage_root,
            stage_name,
            stage_path,
            destination_root,
            relative,
            lock,
        } = prepared;
        let (stage_path, stage_allocation) = stage_path;
        let materialize_options = MaterializeOptions {
            destination: stage_path,
            expected_root_identity: Some(stage_root.identity()),
            maximum_directory_entries: options.maximum_directory_entries,
            maximum_extent_spans: options.maximum_extent_spans,
            transfer_bytes: options.transfer_bytes,
        };
        let materialized = async {
            let mut receipt = GenerationReceipt::from_work(&native_work)?;
            let path = generation_namespace_from_host(
                &relative.0,
                checkout.volume_config(),
                &mut receipt,
            )?;
            materialize_source(
                GenerationSource::Standalone(checkout),
                std::slice::from_ref(&path.0),
                &materialize_options,
                MaterializeMode::DurableOutput,
                receipt,
                None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>,
            )
            .await
            .map(|_| ())
        }
        .await;
        let stage_path = (materialize_options.destination, stage_allocation);
        host_io(Some(lock), native_work.clone(), move |work| {
            let mut cleanup_slot = cleanup_slot;
            let relative = relative;
            let stage_name = stage_name;
            // Every destination operation retains the originally bound
            // root and parent; publication never reopens an ambient path.
            let _root = root;
            let _destination_root = destination_root;
            let _stage_path = stage_path;
            let name = relative
                .0
                .file_name()
                .ok_or(MaterializeError::InvalidPath)?;
            let result = match materialized {
                Ok(_) => publish_restore(
                    &destination_parent,
                    &stage_root,
                    &relative.0,
                    replacement,
                    work,
                )
                .map(|()| HostPathRestore::Restored),
                Err(MaterializeError::MissingPath) => {
                    remove_restored_path(&destination_parent, Path::new(name), &relative.0, work)
                        .map(|()| HostPathRestore::Removed)
                }
                Err(error) => Err(error),
            };
            let stage_identity = stage_root.identity();
            drop(stage_root);
            let cleanup = stage_owner
                .remove_identity(&stage_name.0, stage_identity, work)
                .and_then(|()| {
                    crate::native_host::sync_directory_held(&stage_owner.directory, work)
                });
            match (result, cleanup) {
                (Ok(value), Ok(())) => Ok(value),
                (Ok(_), Err(error)) => Err(restore_demand_error(error)),
                (Err(error), Ok(())) => Err(error),
                (Err(primary), Err(cleanup)) => {
                    *cleanup_slot.0 = primary;
                    Err(MaterializeError::RestoreCleanup {
                        primary: cleanup_slot.0,
                        cleanup,
                        allocation: cleanup_slot.1,
                    })
                }
            }
        })
        .await
        .map_err(|error| {
            materialize_native_error(crate::NativeTreeMaterializationError::from_runtime_error(
                error,
            ))
        })?
    }
    .await;
    let spent = native_work.receipt();
    result
        .map(|value| OperationReceipt { value, work: spent })
        .map_err(|error| OperationFailure::new(error, spent))
}
