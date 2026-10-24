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
) -> Result<OperationReceipt<MaterializationReceipt>, E>
where
    F: Future<Output = Result<OperationReceipt<MaterializationReceipt>, E>>,
    E: From<MaterializeError>,
{
    // Retire the stack-safety box before releasing its allocation.
    receipt
        .custody
        .grow((std::mem::size_of::<F>()) as u64)
        .map_err(MaterializeError::from)
        .map_err(E::from)?;
    let custody = receipt
        .custody
        .split_off((std::mem::size_of::<F>()) as u64)
        .map_err(MaterializeError::from)
        .map_err(E::from)?;
    let meter = receipt.custody.work().clone();
    let mut future = in_heap(move || create(receipt));
    let result = future.as_mut().await;
    drop(future);
    drop(custody);
    let work = meter.receipt();
    result.map(|mut receipt| {
        receipt.work = work;
        receipt
    })
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
    let original =
        crate::NativeMaterializationWork::new(budget, cancellation).map_err(|(error, work)| {
            OperationFailure {
                error: MaterializeError::from(error),
                work,
            }
        })?;
    let receipt = GenerationReceipt::from_work(&original)
        .map_err(|error| OperationFailure::new(error, original.receipt()))?;
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
    .map_err(|error| OperationFailure::new(error, original.receipt()))
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

fn select_materialization_paths<'a>(
    paths: &'a [NamespacePath],
    receipt: &mut GenerationReceipt,
) -> Result<Vec<&'a NamespacePath>, MaterializeError> {
    let mut selected_paths = Vec::<&NamespacePath>::new();
    receipt.reserve(&mut selected_paths, paths.len())?;
    for path in paths {
        let mut insertion = selected_paths.len();
        while insertion != 0 {
            let previous = *selected_paths
                .get(insertion - 1)
                .ok_or(MaterializeError::InvalidOptions)?;
            receipt
                .custody
                .work()
                .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
                .map_err(MaterializeError::from)?;
            if previous <= path {
                break;
            }
            insertion -= 1;
        }
        receipt
            .custody
            .work()
            .charge(crate::materializer::NativeResourceCredit::Copy.delta(
                ((selected_paths.len() - insertion + 1) * std::mem::size_of::<&NamespacePath>())
                    as u64,
            ))
            .map_err(MaterializeError::from)?;
        selected_paths.insert(insertion, path);
    }
    for pair in selected_paths.windows(2) {
        receipt
            .custody
            .work()
            .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
            .map_err(MaterializeError::from)?;
        if matches!(pair, [ancestor, path] if path.is_within(ancestor)) {
            return Err(MaterializeError::OverlappingPaths);
        }
    }
    Ok(selected_paths)
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
    let mut work = receipt.custody.work().clone();
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
    let selected_paths = select_materialization_paths(paths, &mut receipt)?;
    let selection_bytes = vec_bytes(&selected_paths);
    let destination = validate_options(options, &mut receipt)?;
    let host_root = receipt.owned_arc(destination, 0)?;
    let mut known_files = Vec::<(FileId, HeldHostPath)>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = Vec::<((u64, ObjectId, ObjectId), HeldHostPath)>::new();
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    let limits = reader.volume_config().limits;
    for path in selected_paths {
        let host_path = materialize_host_path(None, path.components(), &mut receipt)?;
        if path.is_root() {
            let namespace = clone_namespace(path, None, limits, &mut receipt)?;
            let bytes = usize::try_from(namespace.retained_bytes())
                .map_err(|_| MaterializeError::Work(WorkError::Overflow))?;
            let namespace = receipt.owned_arc(namespace, bytes)?;
            receipt.push(&mut pending, (namespace.clone(), host_path.clone()))?;
            receipt.push(&mut deferred_directory_metadata, (namespace, host_path))?;
            continue;
        }
        let record = match &mut source {
            GenerationSource::Standalone(checkout) => {
                let remaining = receipt
                    .custody
                    .work()
                    .remaining()
                    .map_err(MaterializeError::from)?;
                let lookup = checkout
                    .lookup_no_follow(path, remaining, cancellation)
                    .await
                    .map_err(|failure| receipt.engine_failure(failure))?;
                work.merge(lookup.work).map_err(MaterializeError::from)?;
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
        #[cfg(unix)]
        if let Some(parent) = super::work::host_path(&host_path.0)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            host_root
                .0
                .create_dir_all_held(parent, &mut work)
                .map_err(MaterializeError::from)?;
        }
        #[cfg(windows)]
        if let Some(separator) = host_path
            .0
            .iter()
            .rposition(|unit| *unit == u16::from(b'\\'))
        {
            host_root
                .0
                .create_dir_all_held(
                    host_path
                        .0
                        .get(..separator)
                        .ok_or(MaterializeError::InvalidOptions)?,
                    &mut work,
                )
                .map_err(MaterializeError::from)?;
        }
        generation_action(
            &mut before_effect,
            NativeMaterializationActionPhase::Completed,
        )
        .await?;
        let directory = if record.kind == FileKind::Directory {
            let namespace = clone_namespace(path, None, limits, &mut receipt)?;
            let bytes = usize::try_from(namespace.retained_bytes())
                .map_err(|_| MaterializeError::Work(WorkError::Overflow))?;
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
            &record,
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
        .map_err(MaterializeError::from)?;
    while let Some((directory, host_directory)) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let page = match &mut source {
                GenerationSource::Standalone(checkout) => {
                    let remaining = receipt
                        .custody
                        .work()
                        .remaining()
                        .map_err(MaterializeError::from)?;
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
                    work.merge(page.work).map_err(MaterializeError::from)?;
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
                let child = clone_namespace(&directory.0, Some(&entry.name), limits, &mut receipt)?;
                let child_host = materialize_host_path(
                    Some(&host_directory.0),
                    std::slice::from_ref(&entry.name),
                    &mut receipt,
                )?;
                let child_bytes = usize::try_from(child.retained_bytes())
                    .map_err(|_| MaterializeError::Work(WorkError::Overflow))?;
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
                    &entry.record,
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
                        .map_err(MaterializeError::from)?;
                }
                if let Some(previous) = after.replace(entry.name) {
                    let bytes = previous.retained_bytes();
                    drop(previous);
                    receipt
                        .custody
                        .release(bytes as u64)
                        .map_err(MaterializeError::from)?;
                }
            }
            receipt
                .custody
                .release(entry_bytes as u64)
                .map_err(MaterializeError::from)?;
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
                .map_err(MaterializeError::from)?;
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            &reader,
            &host_root,
            &directory.0,
            &host_directory,
            None,
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
                    .map_err(MaterializeError::from)?,
            );
            for relative in relatives {
                let mut path = super::selection::generation_namespace_from_selection(
                    relative,
                    reader.volume_config(),
                    &mut receipt,
                )?;
                paths
                    .1
                    .absorb(&mut path.1)
                    .map_err(MaterializeError::NativeWork)?;
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
}

pub(super) fn generation_namespace_from_host(
    relative: &Path,
    config: crate::VolumeConfig,
    receipt: &mut GenerationReceipt,
) -> Result<(NamespacePath, crate::NativeAllocation), MaterializeError> {
    let limits = config.limits;
    let mut count = 0_usize;
    for component in relative.components() {
        receipt
            .custody
            .work()
            .charge(crate::materializer::NativeResourceCredit::Items.delta(1))
            .map_err(MaterializeError::from)?;
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
        receipt
            .custody
            .work()
            .charge(WorkCounters {
                items_examined: 1,
                source_path_components: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(MaterializeError::from)?;
        let encoded = crate::native_name::host_name_bytes_with_native_work(
            component.as_os_str(),
            config.profile,
            limits.maximum_component_bytes,
            receipt.custody.work(),
        );
        let ((encoding, bytes), custody) = encoded.map_err(MaterializeError::from)?;
        let bytes = receipt.accept_source(bytes, custody)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| MaterializeError::InvalidPath)?;
        names.push(logical);
    }
    let path = NamespacePath::new(names, limits).map_err(|_| MaterializeError::InvalidPath)?;
    let retained = usize::try_from(path.retained_bytes()).map_err(|_| WorkError::Overflow)?;
    Ok((
        path,
        receipt
            .custody
            .split_off(retained as u64)
            .map_err(MaterializeError::from)?,
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
        .map_err(|(error, work)| OperationFailure::new(MaterializeError::from(error), *work))?;
    let result = async {
        validate_bounds(options)?;
        let mut receipt = GenerationReceipt::from_work(&native_work)?;
        let path =
            generation_namespace_from_host(relative, checkout.volume_config(), &mut receipt)?;
        let allocation_work = native_work.clone();
        let cleanup_allocation = allocation_work
            .claim(
                std::mem::size_of::<MaterializeError>() as u64,
                u64::from((std::mem::size_of::<MaterializeError>() as u64) != 0),
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MaterializeError::from)?;
        let cleanup_slot = (
            Box::new(MaterializeError::InvalidOptions),
            cleanup_allocation,
        );
        let destination_root = allocation_work
            .path(&options.destination, None)
            .map_err(MaterializeError::from)?;
        let relative = allocation_work
            .path(relative, None)
            .map_err(MaterializeError::from)?;
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
        .map_err(MaterializeError::from)??;
        let (stage_path, stage_allocation) = prepared.stage_path;
        let materialize_options = MaterializeOptions {
            destination: stage_path,
            expected_root_identity: Some(prepared.stage_root.identity()),
            maximum_directory_entries: options.maximum_directory_entries,
            maximum_extent_spans: options.maximum_extent_spans,
            transfer_bytes: options.transfer_bytes,
        };
        let materialized = materialize_source(
            GenerationSource::Standalone(checkout),
            std::slice::from_ref(&path.0),
            &materialize_options,
            MaterializeMode::DurableOutput,
            receipt,
            None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>,
        )
        .await
        .map(|_| ());
        drop(path);
        let prepared = PreparedRestore {
            stage_path: (materialize_options.destination, stage_allocation),
            ..prepared
        };
        finish_restore(
            prepared,
            replacement,
            materialized,
            cleanup_slot,
            native_work.clone(),
        )
        .await
    }
    .await;
    let spent = native_work.receipt();
    result
        .map(|value| OperationReceipt { value, work: spent })
        .map_err(|error| OperationFailure::new(error, spent))
}
