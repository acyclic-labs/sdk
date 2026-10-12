use super::*;

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one entry needs the reader, host root, both paths, its record, options, budget, cancellation, and the shared link and pending maps"
)]
pub(super) async fn materialize_entry<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<(HostRoot, crate::NativeAllocation)>,
    path: &NamespacePath,
    host_path: &Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    directory: Option<Arc<(NamespacePath, crate::NativeAllocation)>>,
    file_id: FileId,
    kind: FileKind,
    payload: FilePayload,
    #[cfg(any(target_os = "macos", windows))] metadata_id: ObjectId,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
    known_files: &mut Vec<(FileId, Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>)>,
    #[cfg(any(target_os = "macos", windows))] known_payloads: &mut Vec<(
        (u64, ObjectId, ObjectId),
        Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    )>,
    pending: &mut Vec<(
        Arc<(NamespacePath, crate::NativeAllocation)>,
        Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>,
    )>,
    receipt: &mut GenerationReceipt,
    mode: MaterializeMode,
    before_effect: &mut Option<&mut F>,
) -> Result<(), E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    receipt.run(|work| {
        work.with_original_leaf(|work| {
            work.check()?;
            work.work.admit(
                &WorkCounters {
                    materializations: 1,
                    ..WorkCounters::UNCHARGED
                },
                &work.budget,
            )?;
            Ok(())
        })
    })?;
    for (identity, existing) in known_files.iter() {
        receipt.run(|work| {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
        })?;
        if *identity == file_id {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            receipt.run(|work| {
                #[cfg(unix)]
                {
                    host_root.0.hard_link(
                        super::work::host_path(&existing.0),
                        super::work::host_path(&host_path.0),
                        work,
                    )
                }
                #[cfg(windows)]
                {
                    host_root
                        .0
                        .hard_link_wide_measured(&existing.0, &host_path.0, work)
                }
            })?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.files = receipt
                .value
                .files
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.run(|work| work.check())?;
            return Ok(());
        }
    }
    let mut authenticated_metadata = None;
    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            receipt.run(|work| {
                #[cfg(unix)]
                {
                    host_root
                        .0
                        .create_dir(super::work::host_path(&host_path.0), work)
                }
                #[cfg(windows)]
                {
                    host_root.0.create_dir_wide_measured(&host_path.0, work)
                }
            })?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            let directory = directory.ok_or(MaterializeError::InvalidOptions)?;
            receipt.value.directories = receipt
                .value
                .directories
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
            receipt.push(pending, (directory, host_path.clone()))?;
            receipt.run(|work| work.check())?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            let file = create_file(&host_root.0, &host_path.0, receipt)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            let length = data.as_bytes().len();
            let original = receipt.custody.work().clone();
            let (bytes, allocation) = original
                .copy_bytes(data.as_bytes())
                .map_err(materialize_native_error)?;
            let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
                bytes,
                allocation,
                original.runtime_meter(),
            )
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    original.runtime_error(error)
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    crate::NativeTreeMaterializationError::InvalidLayout
                }
            });
            let bytes = bytes.map_err(materialize_native_error)?;
            let written =
                write_generation_bytes(&file, 0, bytes, 0, receipt, before_effect).await?;
            if mode == MaterializeMode::DurableOutput {
                sync_generation_file(&file, receipt, before_effect).await?;
            }
            drop(file);
            account_file(receipt, length as u64, written)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes,
                extents,
            },
        ) => {
            #[cfg(not(any(target_os = "macos", windows)))]
            let _ = extents;
            #[cfg(any(target_os = "macos", windows))]
            let mut cloned = false;
            #[cfg(any(target_os = "macos", windows))]
            let mut payload_known = false;
            #[cfg(any(target_os = "macos", windows))]
            for (key, source) in known_payloads.iter() {
                receipt.run(|work| {
                    work.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })
                })?;
                if *key == (logical_bytes, extents, metadata_id) {
                    payload_known = true;
                    generation_action(before_effect, NativeMaterializationActionPhase::Before)
                        .await?;
                    cloned = receipt.run(|work| {
                        #[cfg(target_os = "macos")]
                        {
                            host_root.0.clone_file_from(
                                &host_root.0,
                                super::work::host_path(&source.0),
                                super::work::host_path(&host_path.0),
                                work,
                            )
                        }
                        #[cfg(windows)]
                        {
                            host_root.0.clone_file_from_wide_measured(
                                &host_root.0,
                                &source.0,
                                &host_path.0,
                                work,
                            )
                        }
                    })?;
                    generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                        .await?;
                    break;
                }
            }
            #[cfg(not(any(target_os = "macos", windows)))]
            let cloned = false;
            if !cloned {
                generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
                let mut file = create_file(&host_root.0, &host_path.0, receipt)?;
                generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                    .await?;
                #[cfg(windows)]
                {
                    generation_action(before_effect, NativeMaterializationActionPhase::Before)
                        .await?;
                    let meter = receipt.custody.work().runtime_meter();
                    let control = file
                        .control_async(
                            |file, meter| {
                                mark_sparse_measured(file, meter.expect("metered control"))
                            },
                            Some(meter),
                        )
                        .map_err(materialize_runtime_error)?;
                    control.await.map_err(materialize_runtime_error)?;
                    generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                        .await?;
                }
                generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
                let meter = receipt.custody.work().runtime_meter();
                let resize = file
                    .set_len_async(logical_bytes, Some(meter))
                    .map_err(materialize_runtime_error)?;
                resize.await.map_err(materialize_runtime_error)?;
                generation_action(before_effect, NativeMaterializationActionPhase::Completed)
                    .await?;
                authenticated_metadata = Some(
                    materialize_sparse_file(
                        reader,
                        path,
                        &mut file,
                        logical_bytes,
                        options,
                        cancellation,
                        receipt,
                        before_effect,
                    )
                    .await?,
                );
                if mode == MaterializeMode::DurableOutput {
                    sync_generation_file(&file, receipt, before_effect).await?;
                }
                drop(file);
            }
            #[cfg(any(target_os = "macos", windows))]
            if cloned {
                if mode == MaterializeMode::DurableOutput {
                    #[cfg(target_os = "macos")]
                    let file = receipt
                        .run(|work| {
                            host_root
                                .0
                                .open_file(super::work::host_path(&host_path.0), work)
                        })?
                        .into_std();
                    #[cfg(windows)]
                    let file = receipt
                        .run(|work| {
                            host_root
                                .0
                                .open_file_positional_wide_with_work(&host_path.0, work)
                        })?
                        .into_std();
                    let meter = receipt.custody.work().runtime_meter();
                    let file = acyclic_native_runtime::NativeFile::from_file(
                        file,
                        Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
                    )
                    .map_err(|(error, file)| {
                        drop(file);
                        error
                    })
                    .map_err(materialize_runtime_error)?;
                    sync_generation_file(&file, receipt, before_effect).await?;
                    drop(file);
                }
            }
            account_file(receipt, logical_bytes, 0)?;
            #[cfg(any(target_os = "macos", windows))]
            if !payload_known {
                receipt.push(
                    known_payloads,
                    ((logical_bytes, extents, metadata_id), host_path.clone()),
                )?;
            }
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            let target = read_source!(reader, receipt, remaining, value,
                reader.read_symbolic_link(path, receipt.custody.work(), cancellation) => value,
                reader.read_symbolic_link(path, remaining, cancellation) => {
                    receipt.adopt(value.len())?;
                    value
                },
            );
            if target.len() as u64 != target_bytes {
                return Err(MaterializeError::Engine("symbolic-link length mismatch").into());
            }
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            create_symlink_measured(&host_root.0, &target, &host_path.0, receipt)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            drop(target);
            if !reader.validate_native_work(receipt.custody.work()).is_ok() {
                receipt
                    .custody
                    .release((target_bytes as usize) as u64)
                    .map_err(materialize_native_error)?;
            }
            receipt.value.symbolic_links = receipt
                .value
                .symbolic_links
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (kind @ (FileKind::Fifo | FileKind::Socket), FilePayload::Empty) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            create_special_measured(&host_root.0, &host_path.0, kind, None, receipt)?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.special_files = receipt
                .value
                .special_files
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (
            kind @ (FileKind::CharacterDevice | FileKind::BlockDevice),
            FilePayload::Device { major, minor },
        ) => {
            generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
            create_special_measured(
                &host_root.0,
                &host_path.0,
                kind,
                Some((major, minor)),
                receipt,
            )?;
            generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.special_files = receipt
                .value
                .special_files
                .checked_add(1)
                .ok_or(MaterializeError::Work(WorkError::Overflow))?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (unsupported, _) => return Err(MaterializeError::UnsupportedKind(unsupported).into()),
    }
    apply_metadata(
        reader,
        host_root,
        path,
        host_path,
        authenticated_metadata,
        cancellation,
        receipt,
        before_effect,
    )
    .await?;
    receipt.run(|work| work.check())?;
    Ok(())
}
